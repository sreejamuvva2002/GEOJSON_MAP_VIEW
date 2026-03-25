from backend.rag_pipeline import HybridGeospatialRAGPipeline


def test_citation_validator_rejects_uncited_bullets() -> None:
    pipeline = HybridGeospatialRAGPipeline.__new__(HybridGeospatialRAGPipeline)
    retrieved_chunks = [
        {
            "evidence_id": "DOC:alpha:0:company_profile",
            "text": "Alpha Inside is a battery supplier.",
        }
    ]

    answer = "- Alpha Inside is a battery supplier.\n- Evidence Gaps: minimal."
    validated = pipeline._validate_answer_citations(answer=answer, mode="eval", retrieved_chunks=retrieved_chunks)

    assert validated.startswith("Abstaining because")
