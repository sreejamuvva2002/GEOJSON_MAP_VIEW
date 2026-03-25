from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import List, Set, Tuple

import faiss
import numpy as np
import pandas as pd

from backend.ingestion import _hash_embed_one

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FAISS_PATH = PROJECT_ROOT / "data" / "gnem_faiss.index"
DEFAULT_METADATA_PATH = PROJECT_ROOT / "data" / "vector_metadata.json"
STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "that",
    "this",
    "from",
    "into",
    "are",
    "was",
    "were",
    "which",
    "near",
    "within",
    "companies",
    "company",
    "km",
}


def _tokenize(text: str) -> Set[str]:
    tokens = set(re.findall(r"[a-z0-9]+", text.lower()))
    return {t for t in tokens if len(t) > 2 and t not in STOPWORDS}


class VectorEngine:
    def __init__(
        self,
        faiss_path: Path = DEFAULT_FAISS_PATH,
        metadata_path: Path = DEFAULT_METADATA_PATH,
    ) -> None:
        self.faiss_path = Path(faiss_path)
        self.metadata_path = Path(metadata_path)

        if not self.faiss_path.exists():
            raise FileNotFoundError(f"FAISS index not found: {self.faiss_path}")
        if not self.metadata_path.exists():
            raise FileNotFoundError(f"Vector metadata not found: {self.metadata_path}")

        self.index = faiss.read_index(str(self.faiss_path))
        self.dimension = int(self.index.d)
        self.records, self.embedding_backend, self.embedding_model = self._load_metadata()
        self.embed_mode, self.model = self._init_embedder()

    def _load_metadata(self) -> Tuple[List[dict], str, str]:
        payload = json.loads(self.metadata_path.read_text(encoding="utf-8"))
        records = payload.get("records", [])
        embedding_backend = str(payload.get("embedding_backend") or "").strip().lower()
        embedding_model = str(payload.get("embedding_model") or "").strip()
        if not embedding_backend or not embedding_model:
            raise RuntimeError("Vector metadata is missing embedding backend/model information.")
        return records, embedding_backend, embedding_model

    def _init_embedder(self) -> Tuple[str, object]:
        runtime_backend = os.getenv("EMBEDDING_BACKEND")
        if runtime_backend and runtime_backend.strip().lower() not in {
            self.embedding_backend,
            "sentence" if self.embedding_backend == "sentence-transformers" else self.embedding_backend,
            "sbert" if self.embedding_backend == "sentence-transformers" else self.embedding_backend,
        }:
            raise RuntimeError(
                "Embedding backend mismatch between runtime configuration and vector metadata. "
                f"Metadata backend={self.embedding_backend}, runtime backend={runtime_backend!r}."
            )

        if self.embedding_backend == "hash":
            return "hash", None

        if self.embedding_backend == "sentence-transformers":
            from sentence_transformers import SentenceTransformer

            local_only = os.getenv("EMBEDDING_LOCAL_ONLY", "true").strip().lower() == "true"
            model = SentenceTransformer(self.embedding_model, local_files_only=local_only)
            return "sentence-transformers", model

        raise RuntimeError(
            f"Unsupported embedding backend in metadata: {self.embedding_backend}. "
            "Re-run ingestion with a supported deterministic backend."
        )

    def _embed_query(self, query: str) -> np.ndarray:
        if self.embed_mode == "sentence-transformers" and self.model is not None:
            vec = self.model.encode(
                [query],
                normalize_embeddings=True,
                convert_to_numpy=True,
            ).astype(np.float32)
            return vec

        if self.embed_mode == "hash":
            vec = _hash_embed_one(query, dim=self.dimension)
            return vec.reshape(1, -1).astype(np.float32)

        raise RuntimeError(f"Unsupported query embedding mode: {self.embed_mode}")

    def semantic_company_search(self, query: str, top_k: int = 10, per_company_limit: int = 4) -> pd.DataFrame:
        if not self.records:
            return pd.DataFrame()

        k = max(1, min(max(int(top_k) * 5, 40), len(self.records)))
        q_vec = self._embed_query(query)
        faiss.normalize_L2(q_vec)
        scores, indices = self.index.search(q_vec, k)

        query_terms = _tokenize(query)
        rows = []
        for score, idx in zip(scores[0], indices[0]):
            if idx < 0 or idx >= len(self.records):
                continue
            record = dict(self.records[idx])
            chunk_text = str(record.get("chunk_text", "")).strip()
            lexical_score = self._lexical_overlap(query_terms, chunk_text)

            semantic_score = float(score)
            semantic_norm = max(0.0, min(1.0, (semantic_score + 1.0) / 2.0))
            hybrid_score = 0.8 * semantic_norm + 0.2 * lexical_score

            record["semantic_score"] = semantic_score
            record["lexical_score"] = float(lexical_score)
            record["hybrid_score"] = float(hybrid_score)
            rows.append(record)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        if "chunk_id" in df.columns:
            df = df.drop_duplicates(subset=["chunk_id"], keep="first")

        df = df.sort_values(["hybrid_score", "semantic_score"], ascending=[False, False]).reset_index(drop=True)
        if per_company_limit > 0 and "company" in df.columns:
            df = df.groupby("company", group_keys=False).head(int(per_company_limit)).reset_index(drop=True)

        return df.head(int(top_k)).reset_index(drop=True)

    @staticmethod
    def _lexical_overlap(query_terms: Set[str], chunk_text: str) -> float:
        if not query_terms:
            return 0.0
        text_terms = _tokenize(chunk_text)
        if not text_terms:
            return 0.0
        overlap = len(query_terms.intersection(text_terms))
        return overlap / max(1, len(query_terms))
