from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from backend.rag_pipeline import HybridGeospatialRAGPipeline

app = FastAPI(title="Hybrid Geospatial RAG Chatbot", version="0.1.0")

_pipeline: Optional[HybridGeospatialRAGPipeline] = None
_pipeline_error: Optional[str] = None


class ChatRequest(BaseModel):
    question: str = Field(..., min_length=1, description="User question")


class ChatResponse(BaseModel):
    answer: str
    sources: List[str]
    retrieved_chunks: List[Dict[str, Any]]
    retrieved_companies: List[Dict[str, Any]]
    plan: Dict[str, Any]
    model_used: str


@app.on_event("startup")
def startup_event() -> None:
    global _pipeline, _pipeline_error
    try:
        _pipeline = HybridGeospatialRAGPipeline()
        _pipeline_error = None
    except Exception as exc:
        _pipeline = None
        _pipeline_error = str(exc)


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
        result = _pipeline.answer_question(question)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return ChatResponse(**result)
