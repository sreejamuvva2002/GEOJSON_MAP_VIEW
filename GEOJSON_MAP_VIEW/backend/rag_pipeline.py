from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from openai import OpenAI

from backend.analytics_engine import AnalyticsEngine
from backend.geo_utils import PROJECTED_CRS, canonical_county_display_name, normalize_county_name, stable_company_slug
from backend.query_planner import QueryPlanner
from backend.spatial_engine import SpatialEngine
from backend.sql_engine import SQLEngine
from backend.vector_engine import VectorEngine

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "gnem.duckdb"
DEFAULT_GEOJSON_PATH = PROJECT_ROOT / "data" / "Counties_Georgia.geojson"
DEFAULT_FAISS_PATH = PROJECT_ROOT / "data" / "gnem_faiss.index"
DEFAULT_METADATA_PATH = PROJECT_ROOT / "data" / "vector_metadata.json"
DEFAULT_INGESTION_METADATA_PATH = PROJECT_ROOT / "data" / "ingestion_run_metadata.json"
QUESTION_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "about",
    "capital",
    "does",
    "explain",
    "for",
    "from",
    "give",
    "how",
    "in",
    "is",
    "it",
    "list",
    "me",
    "of",
    "please",
    "show",
    "tell",
    "the",
    "to",
    "what",
    "when",
    "where",
    "which",
    "who",
    "why",
}
_CITATION_RE = re.compile(r"\[(DOC:[^\]]+|GEO:[^\]]+|ANALYTIC:[^\]]+)\]")


class HybridGeospatialRAGPipeline:
    def __init__(
        self,
        db_path: Path = DEFAULT_DB_PATH,
        geojson_path: Path = DEFAULT_GEOJSON_PATH,
        faiss_path: Path = DEFAULT_FAISS_PATH,
        metadata_path: Path = DEFAULT_METADATA_PATH,
    ) -> None:
        self.db_path = Path(db_path)
        self.geojson_path = Path(geojson_path)
        self.sql_engine = SQLEngine(db_path=db_path)
        self.analytics_engine = AnalyticsEngine(db_path=db_path)
        self.analytics_engine.ensure_required_tables()
        self.spatial_engine = SpatialEngine(db_path=db_path, geojson_path=geojson_path)
        self.vector_engine = VectorEngine(faiss_path=faiss_path, metadata_path=metadata_path)
        self.query_planner = QueryPlanner()
        self.ingestion_metadata = self._load_ingestion_metadata()
        self.default_mode = os.getenv("MODE", "eval").strip().lower()

        self.llm_base_url = os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434/v1")
        self.llm_client = OpenAI(
            base_url=self.llm_base_url,
            api_key=os.getenv("OLLAMA_API_KEY", "ollama"),
            timeout=float(os.getenv("OLLAMA_TIMEOUT_SECONDS", "60")),
            max_retries=0,
        )
        self.available_models = self._list_available_models()
        self.llm_model = os.getenv("OLLAMA_MODEL") or self._choose_default_model()
        self.fallback_model_preferences = [
            "qwen2.5:14b",
            "qwen2.5:7b",
            "llama3.1:8b",
            "mistral-small3.2:24b",
            "gemma3:12b",
            "qwen3:8b",
            "qwen3:4b",
            "llama3.2:3b",
            "llama3.2:1b",
            "gemma3:4b",
            "tinyllama:latest",
        ]

    def answer_question(self, question: str, selected_county: Optional[str] = None, mode: Optional[str] = None) -> dict:
        effective_mode = (mode or self.default_mode or "eval").strip().lower()
        plan = self.query_planner.plan(question, selected_county=selected_county)
        route_type = str(plan.get("route_type") or "llm_synthesis")
        hints = dict(plan.get("hints", {}))

        if route_type == "web_needed":
            return self._unsupported_response(question=question, plan=plan, mode=effective_mode)

        if route_type == "lookup":
            result = self._run_lookup_route(question=question, plan=plan)
            return self._build_response(plan=plan, mode=effective_mode, **result)

        if route_type == "analytic_local":
            result = self._run_analytic_route(question=question, plan=plan)
            return self._build_response(plan=plan, mode=effective_mode, **result)

        sql_df = self._run_sql_retrieval(question, hints) if plan.get("sql") else pd.DataFrame()
        vector_df = self._run_vector_retrieval(question, hints) if plan.get("vector") else pd.DataFrame()

        if hints.get("selected_county"):
            sql_df = self._filter_dataframe_to_county(sql_df, str(hints["selected_county"]))
            vector_df = self._filter_dataframe_to_county(vector_df, str(hints["selected_county"]))

        final_df = self._choose_final_results(sql_df=sql_df, vector_df=vector_df)
        if self._should_reject_as_unsupported(question=question, sql_df=sql_df, vector_df=vector_df, final_df=final_df):
            return self._unsupported_response(question=question, plan=plan, mode=effective_mode)

        final_df = self._annotate_map_weights(final_df, question=question, plan=plan)
        retrieved_chunks = self._build_retrieved_chunks(vector_df=vector_df, sql_df=sql_df)
        context = self._format_context(question=question, plan=plan, retrieved_chunks=retrieved_chunks)
        answer = self._generate_answer_with_llm(
            question=question,
            context=context,
            retrieved_chunks=retrieved_chunks,
            mode=effective_mode,
        )
        sources = [self._chunk_source_line(chunk) for chunk in retrieved_chunks[:12]]

        return {
            "answer": answer,
            "sources": sources,
            "retrieved_chunks": retrieved_chunks,
            "retrieved_companies": self._df_to_records(final_df.head(25)),
            "plan": plan,
            "model_used": self.llm_model or "not_called",
            "route_type": route_type,
            "evidence_ids": [chunk["evidence_id"] for chunk in retrieved_chunks],
            "geo_evidence": [],
            "analytic_evidence": [chunk["meta"] for chunk in retrieved_chunks if str(chunk["evidence_id"]).startswith("ANALYTIC:")],
            "mode": effective_mode,
        }

    def _run_lookup_route(self, question: str, plan: Dict[str, object]) -> Dict[str, object]:
        hints = dict(plan.get("hints", {}))
        candidates = self._run_sql_retrieval(question, hints) if self._has_structured_filters(hints) else pd.DataFrame()
        geo_anchor_type = str(plan.get("geo_anchor_type") or "")
        target_county = plan.get("target_county")

        candidate_input = None if candidates.empty else candidates
        if geo_anchor_type == "county" and plan.get("requires_polygon_distance"):
            df = self.spatial_engine.companies_within_miles_of_county(
                county_name=str(target_county),
                miles=float(plan.get("radius_miles") or 0.0),
                candidates=candidate_input,
            )
            operation = "county_distance"
        elif geo_anchor_type == "county":
            df = self.spatial_engine.companies_in_county(
                county_name=str(target_county),
                candidates=candidate_input,
            )
            operation = "county_membership"
        elif geo_anchor_type == "point" and hints.get("coordinates"):
            coords = hints["coordinates"]
            df = self.spatial_engine.companies_within_radius(
                lat=float(coords["lat"]),
                lon=float(coords["lon"]),
                radius_km=float(hints.get("radius_km", 0.0)),
                candidates=candidate_input,
            )
            operation = "point_radius"
        else:
            city_name = str(hints.get("city") or "")
            df = self.spatial_engine.companies_near_city(
                city_name=city_name,
                radius_km=float(hints.get("radius_km", 100.0)),
                candidates=candidate_input,
            )
            operation = "point_radius"

        if df.empty and candidate_input is not None:
            if geo_anchor_type == "county" and plan.get("requires_polygon_distance"):
                df = self.spatial_engine.companies_within_miles_of_county(
                    county_name=str(target_county),
                    miles=float(plan.get("radius_miles") or 0.0),
                    candidates=None,
                )
            elif geo_anchor_type == "county":
                df = self.spatial_engine.companies_in_county(county_name=str(target_county), candidates=None)
            elif geo_anchor_type == "point" and hints.get("coordinates"):
                coords = hints["coordinates"]
                df = self.spatial_engine.companies_within_radius(
                    lat=float(coords["lat"]),
                    lon=float(coords["lon"]),
                    radius_km=float(hints.get("radius_km", 0.0)),
                    candidates=None,
                )
            else:
                df = self.spatial_engine.companies_near_city(
                    city_name=str(hints.get("city") or ""),
                    radius_km=float(hints.get("radius_km", 100.0)),
                    candidates=None,
                )

        df = self._apply_structured_filters(df, hints)
        if hints.get("oem"):
            df = self._filter_by_oem(df, str(hints["oem"]))
        if hints.get("selected_county") and not target_county:
            df = self._filter_dataframe_to_county(df, str(hints["selected_county"]))
        df = self._annotate_map_weights(df, question=question, plan=plan)

        if df.empty:
            no_result_chunk = self._build_geo_no_results_chunk(question=question, plan=plan, operation=operation)
            return {
                "answer": self._build_no_results_answer(no_result_chunk),
                "sources": [self._chunk_source_line(no_result_chunk)],
                "retrieved_chunks": [no_result_chunk],
                "retrieved_companies": [],
                "model_used": "not_called",
                "route_type": plan["route_type"],
                "evidence_ids": [no_result_chunk["evidence_id"]],
                "geo_evidence": [no_result_chunk["meta"]],
                "analytic_evidence": [],
            }

        retrieved_chunks, geo_evidence = self._build_geo_chunks(df=df, plan=plan, operation=operation)
        answer = self._build_geo_answer(question=question, plan=plan, df=df, retrieved_chunks=retrieved_chunks)
        return {
            "answer": answer,
            "sources": [self._chunk_source_line(chunk) for chunk in retrieved_chunks[:12]],
            "retrieved_chunks": retrieved_chunks,
            "retrieved_companies": self._df_to_records(df.head(25)),
            "model_used": "not_called",
            "route_type": plan["route_type"],
            "evidence_ids": [chunk["evidence_id"] for chunk in retrieved_chunks],
            "geo_evidence": geo_evidence,
            "analytic_evidence": [],
        }

    def _run_analytic_route(self, question: str, plan: Dict[str, object]) -> Dict[str, object]:
        analytic_metric = str(plan.get("analytic_metric") or "")
        analytic_term = str(plan.get("analytic_term") or "")

        if analytic_metric == "zero_gap":
            df = self.analytics_engine.counties_with_zero_matches(analytic_term)
            retrieved_chunks = self._build_zero_gap_chunks(df=df, term=analytic_term)
            answer = self._build_zero_gap_answer(question=question, df=df, retrieved_chunks=retrieved_chunks, term=analytic_term)
            return {
                "answer": answer,
                "sources": [self._chunk_source_line(chunk) for chunk in retrieved_chunks[:12]],
                "retrieved_chunks": retrieved_chunks,
                "retrieved_companies": self._df_to_records(df.head(50)),
                "model_used": "not_called",
                "route_type": plan["route_type"],
                "evidence_ids": [chunk["evidence_id"] for chunk in retrieved_chunks],
                "geo_evidence": [],
                "analytic_evidence": [chunk["meta"] for chunk in retrieved_chunks],
            }

        df = self.analytics_engine.top_companies_by_metric(analytic_metric, limit=15)
        df = self._annotate_map_weights(df, question=question, plan=plan)
        retrieved_chunks = self._build_metric_chunks(df=df, metric=analytic_metric)
        answer = self._build_metric_answer(question=question, df=df, retrieved_chunks=retrieved_chunks, metric=analytic_metric)
        return {
            "answer": answer,
            "sources": [self._chunk_source_line(chunk) for chunk in retrieved_chunks[:12]],
            "retrieved_chunks": retrieved_chunks,
            "retrieved_companies": self._df_to_records(df.head(25)),
            "model_used": "not_called",
            "route_type": plan["route_type"],
            "evidence_ids": [chunk["evidence_id"] for chunk in retrieved_chunks],
            "geo_evidence": [],
            "analytic_evidence": [chunk["meta"] for chunk in retrieved_chunks],
        }

    def _load_ingestion_metadata(self) -> Dict[str, object]:
        if not DEFAULT_INGESTION_METADATA_PATH.exists():
            return {}
        try:
            return json.loads(DEFAULT_INGESTION_METADATA_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _build_response(self, plan: Dict[str, object], mode: str, **payload: object) -> dict:
        return {
            "plan": plan,
            "mode": mode,
            **payload,
        }

    def _run_sql_retrieval(self, question: str, hints: dict) -> pd.DataFrame:
        if hints.get("metric"):
            return self.sql_engine.get_top_companies_by_metric(str(hints["metric"]), limit=15)
        if hints.get("industry_group"):
            return self.sql_engine.get_companies_by_industry(str(hints["industry_group"]))
        if hints.get("oem") and not any(hints.get(key) for key in ["category_term", "capability_term", "city"]):
            return self.sql_engine.get_companies_by_oem(str(hints["oem"]))
        if any(hints.get(key) for key in ["oem", "category_term", "capability_term", "city"]):
            return self.sql_engine.search_companies(
                oem_name=str(hints["oem"]) if hints.get("oem") else None,
                category_term=str(hints["category_term"]) if hints.get("category_term") else None,
                capability_term=str(hints["capability_term"]) if hints.get("capability_term") else None,
                city_term=str(hints["city"]) if hints.get("city") else None,
                limit=60,
            )

        lower = question.lower()
        if "top" in lower and "employment" in lower:
            return self.sql_engine.get_top_companies_by_metric("employment", limit=15)
        return pd.DataFrame()

    def _run_vector_retrieval(self, question: str, hints: dict) -> pd.DataFrame:
        vector_df = self.vector_engine.semantic_company_search(question, top_k=10, per_company_limit=6)
        vector_df = self._optional_keyword_filter(vector_df, question)
        vector_df = self._apply_structured_filters(vector_df, hints)
        if hints.get("oem"):
            vector_df = self._filter_by_oem(vector_df, str(hints["oem"]))
        return vector_df

    @staticmethod
    def _has_structured_filters(hints: Dict[str, object]) -> bool:
        return any(hints.get(key) for key in ["oem", "category_term", "capability_term", "city"])

    @staticmethod
    def _filter_by_oem(df: pd.DataFrame, oem: str) -> pd.DataFrame:
        if df.empty or "primary_oems" not in df.columns:
            return df
        mask = df["primary_oems"].fillna("").str.lower().str.contains(oem.lower())
        return df[mask].copy()

    @staticmethod
    def _optional_keyword_filter(df: pd.DataFrame, question: str) -> pd.DataFrame:
        if df.empty:
            return df

        lowered = question.lower()
        keyword_map = {
            "battery": ["battery"],
            "supplier": ["supplier", "supply"],
            "oem": ["oem"],
            "employment": ["employment", "employees"],
            "stamping": ["stamping"],
        }

        active_terms: List[str] = []
        for trigger, terms in keyword_map.items():
            if trigger in lowered:
                active_terms.extend(terms)

        if not active_terms:
            return df

        text_cols = ["chunk_text", "product_service", "ev_supply_chain_role", "industry_group", "primary_oems"]
        existing_cols = [col for col in text_cols if col in df.columns]
        if not existing_cols:
            return df

        combined = df[existing_cols].fillna("").astype(str).agg(" ".join, axis=1).str.lower()
        mask = combined.apply(lambda t: any(term in t for term in active_terms))
        filtered = df[mask].copy()
        return filtered if not filtered.empty else df

    def _apply_structured_filters(self, df: pd.DataFrame, hints: dict) -> pd.DataFrame:
        if df.empty:
            return df

        filtered = df.copy()
        if hints.get("category_term") and "category" in filtered.columns:
            mask = filtered["category"].fillna("").str.lower().str.contains(str(hints["category_term"]).lower())
            if mask.any():
                filtered = filtered[mask].copy()

        if hints.get("capability_term"):
            text_cols = [col for col in ["industry_group", "product_service", "ev_supply_chain_role", "chunk_text"] if col in filtered.columns]
            if text_cols:
                combined = filtered[text_cols].fillna("").astype(str).agg(" ".join, axis=1).str.lower()
                mask = combined.str.contains(str(hints["capability_term"]).lower())
                if mask.any():
                    filtered = filtered[mask].copy()

        if hints.get("city"):
            text_cols = [col for col in ["city", "location", "county"] if col in filtered.columns]
            if text_cols:
                combined = filtered[text_cols].fillna("").astype(str).agg(" ".join, axis=1).str.lower()
                mask = combined.str.contains(str(hints["city"]).lower())
                if mask.any():
                    filtered = filtered[mask].copy()

        if hints.get("selected_county"):
            filtered = self._filter_dataframe_to_county(filtered, str(hints["selected_county"]))

        return filtered

    @staticmethod
    def _filter_dataframe_to_county(df: pd.DataFrame, county_name: str) -> pd.DataFrame:
        if df.empty:
            return df
        county_key = normalize_county_name(county_name)
        if not county_key:
            return df
        if "county_key" in df.columns:
            mask = df["county_key"].fillna("").astype(str) == county_key
            return df[mask].copy()
        if "county" in df.columns:
            mask = df["county"].fillna("").apply(normalize_county_name) == county_key
            return df[mask].copy()
        return df

    @staticmethod
    def _choose_final_results(sql_df: pd.DataFrame, vector_df: pd.DataFrame) -> pd.DataFrame:
        if not sql_df.empty and not vector_df.empty:
            merged = pd.concat([sql_df, vector_df], ignore_index=True, sort=False)
            if "company" in merged.columns:
                merged = merged.drop_duplicates(subset=["company"], keep="first")
            return merged
        if not sql_df.empty:
            return sql_df
        if not vector_df.empty:
            return vector_df
        return pd.DataFrame()

    @staticmethod
    def _df_to_records(df: pd.DataFrame) -> List[dict]:
        if df.empty:
            return []
        safe = df.where(pd.notnull(df), None).copy()
        return safe.to_dict(orient="records")

    @classmethod
    def _query_terms(cls, text: str) -> set[str]:
        tokens = set(re.findall(r"[a-z0-9]+", str(text).lower()))
        return {token for token in tokens if len(token) > 2 and token not in QUESTION_STOPWORDS}

    def _should_reject_as_unsupported(
        self,
        question: str,
        sql_df: pd.DataFrame,
        vector_df: pd.DataFrame,
        final_df: pd.DataFrame,
    ) -> bool:
        if not sql_df.empty:
            return False
        if vector_df.empty and final_df.empty:
            return True

        max_lexical = 0.0
        max_hybrid = 0.0
        if not vector_df.empty:
            if "lexical_score" in vector_df.columns:
                max_lexical = float(pd.to_numeric(vector_df["lexical_score"], errors="coerce").fillna(0.0).max())
            if "hybrid_score" in vector_df.columns:
                max_hybrid = float(pd.to_numeric(vector_df["hybrid_score"], errors="coerce").fillna(0.0).max())

        query_terms = self._query_terms(question)
        top_text = " ".join(vector_df.head(5).get("chunk_text", pd.Series(dtype=str)).fillna("").astype(str).tolist())
        overlap_terms = query_terms.intersection(self._query_terms(top_text))

        if max_lexical >= float(os.getenv("RAG_MIN_LEXICAL_SCORE", "0.15")):
            return False
        if overlap_terms:
            return False
        if self.vector_engine.embed_mode == "sentence-transformers" and max_hybrid >= float(
            os.getenv("RAG_MIN_SEMANTIC_HYBRID_SCORE", "0.72")
        ):
            return False
        return True

    def _unsupported_response(self, question: str, plan: Dict[str, object], mode: str) -> dict:
        return {
            "answer": (
                f"I could not find deterministic evidence in the GNEM dataset to answer '{question}'. "
                "Try a county, radius, OEM, role/category, product/service, or employment question."
            ),
            "sources": [],
            "retrieved_chunks": [],
            "retrieved_companies": [],
            "plan": plan,
            "model_used": "not_called",
            "route_type": plan.get("route_type", "web_needed"),
            "evidence_ids": [],
            "geo_evidence": [],
            "analytic_evidence": [],
            "mode": mode,
        }

    def _annotate_map_weights(self, df: pd.DataFrame, question: str, plan: Dict[str, object]) -> pd.DataFrame:
        if df.empty:
            return df

        out = df.copy()
        hints = plan.get("hints", {}) if isinstance(plan, dict) else {}

        out["map_relevance"] = self._compute_relevance_component(out)
        out["map_query_match"] = self._compute_query_match_component(out, question=question)
        out["map_proximity"] = self._compute_proximity_component(out, hints=hints)
        out["map_metric"] = self._compute_metric_component(out)
        out["map_business_priority"] = out.apply(
            lambda row: self._business_priority_score(row, hints=hints),
            axis=1,
        )

        component_weights = {
            "map_relevance": 0.30,
            "map_query_match": 0.25,
            "map_proximity": 0.20,
            "map_business_priority": 0.15,
            "map_metric": 0.10,
        }

        map_weights: List[float] = []
        reasons: List[str] = []
        for _, row in out.iterrows():
            total_weight = 0.0
            weighted_sum = 0.0
            parts: List[str] = []
            for column, column_weight in component_weights.items():
                value = row.get(column)
                if pd.isna(value):
                    continue
                value_f = float(value)
                weighted_sum += value_f * column_weight
                total_weight += column_weight
                if value_f > 0:
                    parts.append(f"{column.replace('map_', '')}={value_f:.2f}")

            score = weighted_sum / total_weight if total_weight > 0 else 0.5
            map_weights.append(round(max(0.05, min(1.0, score)), 4))
            reasons.append(", ".join(parts) if parts else "default=0.50")

        out["map_weight"] = map_weights
        out["map_weight_reason"] = reasons
        if "score" not in out.columns:
            out["score"] = out["map_weight"]
        else:
            out["score"] = pd.to_numeric(out["score"], errors="coerce").fillna(out["map_weight"])
        return out

    @staticmethod
    def _compute_relevance_component(df: pd.DataFrame) -> pd.Series:
        if "hybrid_score" in df.columns:
            return pd.to_numeric(df["hybrid_score"], errors="coerce").fillna(0.0).clip(lower=0.0, upper=1.0)
        if "lexical_score" in df.columns:
            return pd.to_numeric(df["lexical_score"], errors="coerce").fillna(0.0).clip(lower=0.0, upper=1.0)
        if "semantic_score" in df.columns:
            semantic = pd.to_numeric(df["semantic_score"], errors="coerce").fillna(0.0)
            return ((semantic + 1.0) / 2.0).clip(lower=0.0, upper=1.0)
        return pd.Series([pd.NA] * len(df), index=df.index, dtype="object")

    def _compute_query_match_component(self, df: pd.DataFrame, question: str) -> pd.Series:
        query_terms = self._query_terms(question)
        if not query_terms:
            return pd.Series([pd.NA] * len(df), index=df.index, dtype="object")

        text_cols = [
            col
            for col in [
                "company",
                "category",
                "industry_group",
                "ev_supply_chain_role",
                "primary_oems",
                "supplier_or_affiliation_type",
                "product_service",
                "primary_facility_type",
                "chunk_text",
                "city",
                "county",
            ]
            if col in df.columns
        ]
        if not text_cols:
            return pd.Series([pd.NA] * len(df), index=df.index, dtype="object")

        combined = df[text_cols].fillna("").astype(str).agg(" ".join, axis=1)
        scores = []
        for text in combined:
            row_terms = self._query_terms(text)
            overlap = len(query_terms.intersection(row_terms))
            scores.append(overlap / max(1, len(query_terms)))
        return pd.Series(scores, index=df.index, dtype="float64").clip(lower=0.0, upper=1.0)

    @staticmethod
    def _compute_proximity_component(df: pd.DataFrame, hints: Dict[str, object]) -> pd.Series:
        if "distance_km" not in df.columns:
            return pd.Series([pd.NA] * len(df), index=df.index, dtype="object")

        distances = pd.to_numeric(df["distance_km"], errors="coerce")
        if distances.notna().sum() == 0:
            return pd.Series([pd.NA] * len(df), index=df.index, dtype="object")

        radius_hint = float(hints.get("radius_km", 0.0) or 0.0)
        max_distance = float(distances.max(skipna=True) or 0.0)
        scale = max(radius_hint, max_distance, 1.0)
        proximity = 1.0 - (distances / scale)
        return proximity.fillna(pd.NA).clip(lower=0.0, upper=1.0)

    @staticmethod
    def _compute_metric_component(df: pd.DataFrame) -> pd.Series:
        metric_col = "metric_value" if "metric_value" in df.columns else None
        if metric_col is None:
            return pd.Series([pd.NA] * len(df), index=df.index, dtype="object")

        values = pd.to_numeric(df[metric_col], errors="coerce")
        valid = values.dropna()
        if valid.empty:
            return pd.Series([pd.NA] * len(df), index=df.index, dtype="object")
        if float(valid.max()) == float(valid.min()):
            return pd.Series([1.0] * len(df), index=df.index, dtype="float64")

        normalized = (values - float(valid.min())) / max(float(valid.max()) - float(valid.min()), 1e-6)
        return normalized.fillna(pd.NA).clip(lower=0.0, upper=1.0)

    @staticmethod
    def _business_priority_score(row: pd.Series, hints: Dict[str, object]) -> float:
        category = str(row.get("category") or "").lower()
        role = str(row.get("ev_supply_chain_role") or "").lower()
        affiliation = str(row.get("supplier_or_affiliation_type") or "").lower()
        facility = str(row.get("primary_facility_type") or "").lower()
        product = str(row.get("product_service") or "").lower()
        oems = str(row.get("primary_oems") or "").lower()
        combined = " ".join([category, role, affiliation, facility, product, oems])

        score = 0.35
        if "oem" in category or "oem" in role:
            score = max(score, 0.85)
        elif "tier 1" in category or "tier 1" in role:
            score = max(score, 0.75)
        elif "tier 2" in category or "tier 2" in role:
            score = max(score, 0.68)
        elif "supplier" in role or "supplier" in affiliation:
            score = max(score, 0.60)

        if any(term in facility for term in ["manufact", "plant", "assembly", "stamping"]):
            score += 0.10
        if any(term in facility for term in ["warehouse", "distribution", "logistics"]):
            score += 0.05

        category_term = str(hints.get("category_term") or "").lower()
        capability_term = str(hints.get("capability_term") or "").lower()
        oem_term = str(hints.get("oem") or "").lower()
        if category_term and category_term in category:
            score += 0.15
        if capability_term and capability_term in combined:
            score += 0.15
        if oem_term and oem_term in oems:
            score += 0.15

        return round(max(0.0, min(1.0, score)), 4)

    def _build_geo_chunks(self, df: pd.DataFrame, plan: Dict[str, object], operation: str) -> tuple[List[Dict[str, object]], List[Dict[str, object]]]:
        chunks: List[Dict[str, object]] = []
        evidence: List[Dict[str, object]] = []
        county_name = canonical_county_display_name(plan.get("target_county")) if plan.get("target_county") else None

        for rank, (_, row) in enumerate(df.head(12).iterrows(), start=1):
            company = str(row.get("company") or "unknown")
            company_slug = stable_company_slug(company)
            county = county_name or canonical_county_display_name(row.get("county")) or "Unknown"
            evidence_id = f"GEO:{operation}|county={normalize_county_name(county) or 'unknown'}|company={company_slug}"
            distance_mi = None if pd.isna(row.get("distance_miles")) else float(row.get("distance_miles"))
            method = str(row.get("distance_method") or ("polygon_containment" if operation == "county_membership" else "geodesic_point_radius"))
            meta = {
                "evidence_id": evidence_id,
                "operation": operation,
                "county": county,
                "company_id": company_slug,
                "company": company,
                "dist_mi": distance_mi,
                "crs": PROJECTED_CRS if "polygon" in method else "geodesic",
                "method": method,
            }
            text = (
                f"{company} in {row.get('city') or 'Unknown city'}, {county}; "
                f"distance_miles={distance_mi if distance_mi is not None else 0.0:.2f}; "
                f"coordinate_source={row.get('coordinate_source') or 'unknown'}."
            )
            chunks.append(
                {
                    "evidence_id": evidence_id,
                    "engine": "geo",
                    "company": company,
                    "chunk_type": operation,
                    "score": round(1.0 / rank, 4),
                    "text": text,
                    "meta": meta,
                }
            )
            evidence.append(meta)
        return chunks, evidence

    def _build_metric_chunks(self, df: pd.DataFrame, metric: str) -> List[Dict[str, object]]:
        chunks: List[Dict[str, object]] = []
        for rank, (_, row) in enumerate(df.head(12).iterrows(), start=1):
            company = str(row.get("company") or "unknown")
            company_slug = stable_company_slug(company)
            metric_value = row.get("metric_value", row.get(metric))
            evidence_id = f"ANALYTIC:companies|metric={metric}|group={company_slug}"
            chunks.append(
                {
                    "evidence_id": evidence_id,
                    "engine": "analytic",
                    "company": company,
                    "chunk_type": "metric_result",
                    "score": round(1.0 / rank, 4),
                    "text": f"{company} has {metric}={metric_value}.",
                    "meta": {
                        "evidence_id": evidence_id,
                        "table": "companies",
                        "metric": metric,
                        "group": company_slug,
                        "company": company,
                        "value": metric_value,
                    },
                }
            )
        return chunks

    def _build_zero_gap_chunks(self, df: pd.DataFrame, term: str) -> List[Dict[str, object]]:
        chunks: List[Dict[str, object]] = []
        for rank, (_, row) in enumerate(df.head(50).iterrows(), start=1):
            county_name = str(row.get("county_name") or "Unknown")
            evidence_id = f"ANALYTIC:county_gap|metric=zero_gap|group={normalize_county_name(county_name) or 'unknown'}"
            chunks.append(
                {
                    "evidence_id": evidence_id,
                    "engine": "analytic",
                    "company": None,
                    "chunk_type": "zero_gap",
                    "score": round(1.0 / rank, 4),
                    "text": f"{county_name} County has zero matches for '{term}'.",
                    "meta": {
                        "evidence_id": evidence_id,
                        "table": "county_gap",
                        "metric": "zero_gap",
                        "group": county_name,
                        "analytic_term": term,
                    },
                }
            )
        return chunks

    def _build_retrieved_chunks(self, vector_df: pd.DataFrame, sql_df: pd.DataFrame) -> List[Dict[str, object]]:
        chunks: List[Dict[str, object]] = []

        if not sql_df.empty:
            for rank, (_, row) in enumerate(sql_df.head(6).iterrows(), start=1):
                company = str(row.get("company") or "unknown")
                company_slug = stable_company_slug(company)
                evidence_id = f"ANALYTIC:company_lookup|metric=match|group={company_slug}"
                chunks.append(
                    {
                        "evidence_id": evidence_id,
                        "engine": "analytic",
                        "company": company,
                        "chunk_type": "sql_result",
                        "score": round(1.0 / rank, 4),
                        "text": (
                            f"SQL result for {company}: industry={row.get('industry_group')}, "
                            f"role={row.get('ev_supply_chain_role')}, OEMs={row.get('primary_oems')}."
                        ),
                        "meta": {
                            "evidence_id": evidence_id,
                            "table": "companies",
                            "metric": "match",
                            "group": company_slug,
                            "company": company,
                        },
                    }
                )

        if not vector_df.empty:
            for _, row in vector_df.head(8).iterrows():
                doc_ref = str(row.get("chunk_id") or "unknown")
                evidence_id = f"DOC:{doc_ref}"
                chunks.append(
                    {
                        "evidence_id": evidence_id,
                        "engine": "vector",
                        "company": row.get("company"),
                        "chunk_type": row.get("chunk_type", "vector_chunk"),
                        "score": round(float(row.get("hybrid_score", row.get("semantic_score", 0.0))), 4),
                        "text": str(row.get("chunk_text", "")).strip(),
                        "meta": {
                            "evidence_id": evidence_id,
                            "chunk_ref": doc_ref,
                            "semantic_score": row.get("semantic_score"),
                            "lexical_score": row.get("lexical_score"),
                        },
                    }
                )

        seen = set()
        out = []
        for chunk in chunks:
            key = (chunk["evidence_id"], chunk.get("company"), str(chunk.get("text", ""))[:160])
            if key in seen:
                continue
            seen.add(key)
            out.append(chunk)
        return out

    @staticmethod
    def _build_geo_no_results_chunk(question: str, plan: Dict[str, object], operation: str) -> Dict[str, object]:
        county_name = canonical_county_display_name(plan.get("target_county")) if plan.get("target_county") else None
        if county_name:
            evidence_id = f"GEO:{operation}|county={normalize_county_name(county_name) or 'unknown'}|company=no-results"
        else:
            evidence_id = f"GEO:{operation}|county=unknown|company=no-results"
        return {
            "evidence_id": evidence_id,
            "engine": "geo",
            "company": None,
            "chunk_type": "geo_no_results",
            "score": 1.0,
            "text": f"No geo-usable companies matched the deterministic geo query for '{question}'.",
            "meta": {
                "evidence_id": evidence_id,
                "operation": operation,
                "county": county_name,
                "company_id": None,
                "dist_mi": None,
                "crs": PROJECTED_CRS,
                "method": "no_results",
            },
        }

    @staticmethod
    def _build_no_results_answer(chunk: Dict[str, object]) -> str:
        return f"- {chunk['text']} [{chunk['evidence_id']}]"

    def _build_geo_answer(self, question: str, plan: Dict[str, object], df: pd.DataFrame, retrieved_chunks: List[Dict[str, object]]) -> str:
        if df.empty:
            return "No deterministic geo results were found."

        lines = []
        county_name = canonical_county_display_name(plan.get("target_county")) if plan.get("target_county") else None
        if plan.get("requires_polygon_distance") and county_name:
            lines.append(
                f"- Found {len(df)} geo-usable companies within {float(plan.get('radius_miles') or 0.0):.1f} miles of {county_name} County using polygon distance in {PROJECTED_CRS}. [{retrieved_chunks[0]['evidence_id']}]"
            )
        elif county_name:
            lines.append(
                f"- Found {len(df)} geo-usable companies in {county_name} County by point-in-polygon county membership. [{retrieved_chunks[0]['evidence_id']}]"
            )
        else:
            lines.append(
                f"- Found {len(df)} geo-usable companies for the requested point-radius search. [{retrieved_chunks[0]['evidence_id']}]"
            )

        for chunk in retrieved_chunks[:5]:
            company = chunk.get("company") or "Unknown company"
            dist_mi = chunk["meta"].get("dist_mi")
            if dist_mi is not None:
                lines.append(f"- {company} at {dist_mi:.2f} miles. [{chunk['evidence_id']}]")
            else:
                lines.append(f"- {company}. [{chunk['evidence_id']}]")
        return "\n".join(lines)

    def _build_metric_answer(self, question: str, df: pd.DataFrame, retrieved_chunks: List[Dict[str, object]], metric: str) -> str:
        if df.empty:
            return f"- No deterministic {metric} rows were found for '{question}'."
        lines = [f"- Top deterministic {metric} results from DuckDB. [{retrieved_chunks[0]['evidence_id']}]"]
        for chunk in retrieved_chunks[:5]:
            lines.append(f"- {chunk['text']} [{chunk['evidence_id']}]")
        return "\n".join(lines)

    def _build_zero_gap_answer(self, question: str, df: pd.DataFrame, retrieved_chunks: List[Dict[str, object]], term: str) -> str:
        if df.empty:
            return f"- No zero-gap county results were found for '{question}'."
        lines = [f"- Counties with zero '{term}' matches from deterministic county analytics. [{retrieved_chunks[0]['evidence_id']}]"]
        for chunk in retrieved_chunks[:8]:
            lines.append(f"- {chunk['text']} [{chunk['evidence_id']}]")
        return "\n".join(lines)

    def _format_context(self, question: str, plan: dict, retrieved_chunks: List[Dict[str, object]]) -> str:
        lines = [
            f"Question: {question}",
            f"Plan Classification: {plan.get('classification')}",
            f"Route Type: {plan.get('route_type')}",
            "Retrieved Evidence:",
        ]
        for chunk in retrieved_chunks:
            company = chunk.get("company") or "N/A"
            chunk_text = str(chunk.get("text", "")).strip()
            if len(chunk_text) > 180:
                chunk_text = chunk_text[:180] + "..."
            lines.append(f"[{chunk['evidence_id']}] engine={chunk['engine']} | company={company} | type={chunk['chunk_type']}")
            lines.append(chunk_text)
        return "\n".join(lines).strip()

    @staticmethod
    def _chunk_source_line(chunk: Dict[str, object]) -> str:
        snippet = str(chunk.get("text", "")).replace("\n", " ").strip()
        if len(snippet) > 160:
            snippet = snippet[:160] + "..."
        return f"[{chunk.get('evidence_id')}] {chunk.get('engine')} | {chunk.get('company') or 'N/A'} | {snippet}"

    def _generate_answer_with_llm(
        self,
        question: str,
        context: str,
        retrieved_chunks: List[Dict[str, object]],
        mode: str,
    ) -> str:
        if not retrieved_chunks:
            return self._citation_failure_response(mode=mode, retrieved_chunks=[], reason="no evidence")

        self.available_models = self._list_available_models()
        if not self.llm_model and self.available_models:
            self.llm_model = self._choose_default_model()
        if not self.llm_model:
            return self._citation_failure_response(mode=mode, retrieved_chunks=retrieved_chunks, reason="no model")

        system_prompt = (
            "You are a geospatial enterprise analyst. "
            "Answer only from retrieved evidence. Every bullet must include at least one evidence citation token "
            "like [DOC:...] or [ANALYTIC:...]. If evidence is missing, abstain."
        )
        user_prompt = (
            f"{context}\n\n"
            "Instructions:\n"
            "1. Use short bullet points.\n"
            "2. Every non-empty bullet must end with at least one evidence citation.\n"
            "3. Do not fabricate company names, distances, OEM links, or metrics.\n"
            "4. End with one bullet that begins with 'Evidence Gaps:'.\n"
        )
        model_candidates = [self.llm_model] + self._oom_fallback_candidates(self.llm_model)
        max_model_attempts = int(os.getenv("OLLAMA_MAX_MODEL_ATTEMPTS", "2"))
        model_candidates = model_candidates[:max(1, max_model_attempts)]
        last_exc: Optional[Exception] = None
        request_timeout = float(os.getenv("OLLAMA_REQUEST_TIMEOUT_SECONDS", "35"))
        for model_name in model_candidates:
            try:
                response = self.llm_client.chat.completions.create(
                    model=model_name,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    temperature=0.1,
                    max_tokens=180,
                    timeout=request_timeout,
                    extra_body={"options": {"num_predict": 180, "num_ctx": 2048}},
                )
                self.llm_model = model_name
                message = response.choices[0].message if response.choices else None
                content = self._normalize_message_text(getattr(message, "content", None))
                if not content:
                    raise RuntimeError(f"Ollama returned an empty response for model '{model_name}'.")
                return self._validate_answer_citations(answer=content.strip(), mode=mode, retrieved_chunks=retrieved_chunks)
            except Exception as exc:
                last_exc = exc
                if (
                    self._is_memory_error(exc)
                    or self._is_model_unavailable_error(exc)
                    or self._is_timeout_error(exc)
                    or self._is_empty_response_error(exc)
                ):
                    continue
                return self._citation_failure_response(
                    mode=mode,
                    retrieved_chunks=retrieved_chunks,
                    reason=f"llm_error:{exc}",
                )

        return self._citation_failure_response(
            mode=mode,
            retrieved_chunks=retrieved_chunks,
            reason=str(last_exc) if last_exc else "llm_unavailable",
        )

    def _validate_answer_citations(self, answer: str, mode: str, retrieved_chunks: List[Dict[str, object]]) -> str:
        stripped = answer.strip()
        if not stripped:
            return self._citation_failure_response(mode=mode, retrieved_chunks=retrieved_chunks, reason="empty answer")

        lines = [line.strip() for line in stripped.splitlines() if line.strip()]
        bullets = [line for line in lines if re.match(r"^[-*]|\d+\.", line)]
        check_lines = [line for line in bullets if not line.lower().startswith("evidence gaps:")]
        if not check_lines:
            check_lines = [line for line in lines if not line.lower().startswith("evidence gaps:")]

        missing = [line for line in check_lines if not _CITATION_RE.search(line)]
        if not missing:
            return stripped
        return self._citation_failure_response(mode=mode, retrieved_chunks=retrieved_chunks, reason="uncited bullets")

    def _citation_failure_response(self, mode: str, retrieved_chunks: List[Dict[str, object]], reason: str) -> str:
        if mode == "eval":
            return (
                "Abstaining because the final response could not be supported with citation-complete evidence. "
                f"Reason: {reason}."
            )
        return self._deterministic_ui_fallback(retrieved_chunks=retrieved_chunks, reason=reason)

    @staticmethod
    def _deterministic_ui_fallback(retrieved_chunks: List[Dict[str, object]], reason: str) -> str:
        if not retrieved_chunks:
            return f"- No cited evidence was available. Reason: {reason}."
        lines = [f"- Deterministic fallback summary because the narrated answer was rejected. Reason: {reason}. [{retrieved_chunks[0]['evidence_id']}]"]
        for chunk in retrieved_chunks[:5]:
            lines.append(f"- {chunk['text']} [{chunk['evidence_id']}]")
        return "\n".join(lines)

    def _choose_default_model(self) -> Optional[str]:
        preferred = [
            "qwen2.5:14b",
            "qwen3:14b",
            "gpt-oss:20b",
            "mistral-small3.2:24b",
            "llama3.1:8b",
            "gemma3:12b",
            "qwen3:8b",
            "deepseek-r1:14b",
            "deepseek-r1:8b",
            "qwen3:4b",
            "llama3.2:3b",
            "gemma3:4b",
            "tinyllama:latest",
        ]
        model_ids = self.available_models
        if not model_ids:
            return None
        for candidate in preferred:
            if candidate in model_ids:
                return candidate
        return model_ids[0]

    def _list_available_models(self) -> List[str]:
        try:
            models = self.llm_client.models.list()
            return [m.id for m in getattr(models, "data", []) if getattr(m, "id", None)]
        except Exception:
            return []

    def _oom_fallback_candidates(self, current_model: str) -> List[str]:
        available = [m for m in self.available_models if m != current_model]
        ordered: List[str] = []

        for pref in self.fallback_model_preferences:
            if pref in available and pref not in ordered:
                ordered.append(pref)

        current_size = self._model_size_b(current_model)
        dynamic = sorted(available, key=self._model_sort_key)
        for candidate in dynamic:
            candidate_size = self._model_size_b(candidate)
            if current_size is None or candidate_size is None or candidate_size < current_size:
                if candidate not in ordered:
                    ordered.append(candidate)
        return ordered

    @staticmethod
    def _model_size_b(model_name: str) -> Optional[float]:
        match = re.search(r"(\d+(?:\.\d+)?)b", model_name.lower())
        if not match:
            return None
        return float(match.group(1))

    @staticmethod
    def _normalize_message_text(value: object) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        if isinstance(value, list):
            parts: List[str] = []
            for item in value:
                if isinstance(item, str):
                    parts.append(item)
                elif isinstance(item, dict) and item.get("type") == "text" and item.get("text"):
                    parts.append(str(item["text"]))
                elif hasattr(item, "text") and getattr(item, "text"):
                    parts.append(str(getattr(item, "text")))
            return "".join(parts)
        return str(value)

    @classmethod
    def _model_sort_key(cls, model_name: str) -> tuple:
        size = cls._model_size_b(model_name)
        return (size is None, float(size) if size is not None else 10_000.0, model_name)

    @staticmethod
    def _is_memory_error(exc: Exception) -> bool:
        text = str(exc).lower()
        return any(signal in text for signal in ["requires more system memory", "out of memory", "insufficient memory", "cuda out of memory"])

    @staticmethod
    def _is_model_unavailable_error(exc: Exception) -> bool:
        text = str(exc).lower()
        return any(signal in text for signal in ["model not found", "pull model", "does not exist"])

    @staticmethod
    def _is_timeout_error(exc: Exception) -> bool:
        text = str(exc).lower()
        return any(signal in text for signal in ["timed out", "readtimeout", "apitimeouterror"])

    @staticmethod
    def _is_empty_response_error(exc: Exception) -> bool:
        text = str(exc).lower()
        return any(signal in text for signal in ["empty response", "reasoning-only response"])
