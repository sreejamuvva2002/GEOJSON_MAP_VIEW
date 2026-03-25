from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from backend.logging_utils import append_jsonl, build_runtime_metadata
from backend.rag_pipeline import HybridGeospatialRAGPipeline

app = FastAPI(title="Hybrid Geospatial RAG Chatbot", version="0.1.0")

_pipeline: Optional[HybridGeospatialRAGPipeline] = None
_pipeline_error: Optional[str] = None


class ChatRequest(BaseModel):
    question: str = Field(..., min_length=1, description="User question")
    selected_county: Optional[str] = Field(default=None, description="Optional county selected in the UI")
    mode: Optional[str] = Field(default=None, description="Execution mode such as eval or ui")


class ChatResponse(BaseModel):
    answer: str
    sources: List[str]
    retrieved_chunks: List[Dict[str, Any]]
    retrieved_companies: List[Dict[str, Any]]
    plan: Dict[str, Any]
    model_used: str
    route_type: str
    evidence_ids: List[str]
    geo_evidence: List[Dict[str, Any]]
    analytic_evidence: List[Dict[str, Any]]
    mode: str


@app.on_event("startup")
def startup_event() -> None:
    global _pipeline, _pipeline_error
    try:
        _pipeline = HybridGeospatialRAGPipeline()
        _pipeline_error = None
        append_jsonl(
            {
                "event": "startup",
                "status": "ok",
                "details": build_runtime_metadata(
                    planner_route=None,
                    county=None,
                    radius_miles=None,
                    geo_anchor_type=None,
                    evidence_ids=[],
                    configured_crs="EPSG:5070",
                    county_field_trusted=_pipeline.ingestion_metadata.get("COUNTY_FIELD_TRUSTED"),
                    retrieval_summary={},
                    selected_model=_pipeline.llm_model,
                    embedding_backend=_pipeline.vector_engine.embedding_backend,
                    embedding_model=_pipeline.vector_engine.embedding_model,
                    geojson_path=_pipeline.geojson_path,
                    excel_path=Path(_pipeline.ingestion_metadata.get("excel_path")) if _pipeline.ingestion_metadata.get("excel_path") else None,
                    answer_text=None,
                    user_query=None,
                    errors=None,
                ),
            }
        )
    except Exception as exc:
        _pipeline = None
        _pipeline_error = str(exc)
        append_jsonl({"event": "startup", "status": "error", "error": _pipeline_error})


@app.get("/health")
def health() -> dict:
    return {
        "status": "ok" if _pipeline is not None else "error",
        "pipeline_loaded": _pipeline is not None,
        "error": _pipeline_error,
    }


@app.post("/chat", response_model=ChatResponse)
def chat(payload: ChatRequest) -> ChatResponse:
    if _pipeline is None:
        raise HTTPException(
            status_code=500,
            detail=(
                "Pipeline not initialized. Run ingestion first: "
                "python project/backend/ingestion.py. "
                f"Startup error: {_pipeline_error}"
            ),
        )

    question = payload.question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="Question cannot be empty.")

    try:
        result = _pipeline.answer_question(
            question,
            selected_county=payload.selected_county,
            mode=payload.mode,
        )
        append_jsonl(
            {
                "event": "chat",
                "status": "ok",
                "details": build_runtime_metadata(
                    planner_route=result.get("route_type"),
                    county=result.get("plan", {}).get("target_county"),
                    radius_miles=result.get("plan", {}).get("radius_miles"),
                    geo_anchor_type=result.get("plan", {}).get("geo_anchor_type"),
                    evidence_ids=result.get("evidence_ids", []),
                    configured_crs="EPSG:5070",
                    county_field_trusted=_pipeline.ingestion_metadata.get("COUNTY_FIELD_TRUSTED"),
                    retrieval_summary={
                        "retrieved_chunks": len(result.get("retrieved_chunks", [])),
                        "retrieved_companies": len(result.get("retrieved_companies", [])),
                    },
                    selected_model=result.get("model_used"),
                    embedding_backend=_pipeline.vector_engine.embedding_backend,
                    embedding_model=_pipeline.vector_engine.embedding_model,
                    geojson_path=_pipeline.geojson_path,
                    excel_path=Path(_pipeline.ingestion_metadata.get("excel_path")) if _pipeline.ingestion_metadata.get("excel_path") else None,
                    answer_text=result.get("answer"),
                    user_query=question,
                    errors=None,
                ),
            }
        )
    except Exception as exc:
        append_jsonl(
            {
                "event": "chat",
                "status": "error",
                "details": build_runtime_metadata(
                    planner_route=None,
                    county=payload.selected_county,
                    radius_miles=None,
                    geo_anchor_type=None,
                    evidence_ids=[],
                    configured_crs="EPSG:5070",
                    county_field_trusted=_pipeline.ingestion_metadata.get("COUNTY_FIELD_TRUSTED") if _pipeline else None,
                    retrieval_summary={},
                    selected_model=_pipeline.llm_model if _pipeline else None,
                    embedding_backend=_pipeline.vector_engine.embedding_backend if _pipeline else None,
                    embedding_model=_pipeline.vector_engine.embedding_model if _pipeline else None,
                    geojson_path=_pipeline.geojson_path if _pipeline else None,
                    excel_path=Path(_pipeline.ingestion_metadata.get("excel_path")) if _pipeline and _pipeline.ingestion_metadata.get("excel_path") else None,
                    answer_text=None,
                    user_query=question,
                    errors=str(exc),
                ),
            }
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return ChatResponse(**result)
