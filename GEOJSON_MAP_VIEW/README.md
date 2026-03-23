# Hybrid Geospatial RAG Chatbot (Ollama LLM-Only)

This prototype answers questions by combining:

- SQL retrieval over GNEM company data (`DuckDB`)
- Geospatial distance filtering (`geopy`)
- Semantic chunk retrieval (`FAISS` + `sentence-transformers`)
- Final answer generation using a local Ollama model only (no deterministic fallback)

Architecture:

`User -> Streamlit UI -> FastAPI -> Query Planner -> SQL / Geo / Vector Retrieval -> Ollama LLM`

## Project Structure

```text
project/
|-- backend/
|   |-- ingestion.py
|   |-- main.py
|   |-- query_planner.py
|   |-- rag_pipeline.py
|   |-- spatial_engine.py
|   |-- sql_engine.py
|   `-- vector_engine.py
|-- data/
|   |-- gnem_companies.xlsx
|   |-- GNEM - Auto Landscape Lat Long Updated File (1).xlsx
|   `-- Counties_Georgia.geojson
|-- frontend/
|   `-- app.py
|-- requirements.txt
`-- README.md
```

## Install and Run

From `project/`:

```bash
pip install -r requirements.txt
python backend/ingestion.py
uvicorn backend.main:app --reload
streamlit run frontend/app.py
```

## Ollama Setup (Required)

1. Start Ollama (or ensure it is already running):

```bash
ollama serve
```

2. Pull a model:

```bash
ollama pull qwen3:14b
```

3. (Optional) Set explicit model and endpoint:

```bash
set OLLAMA_MODEL=qwen3:14b
set OLLAMA_BASE_URL=http://127.0.0.1:11434/v1
```

If `OLLAMA_MODEL` is not set, backend auto-selects the best available pulled model from your local Ollama registry.

### Suggested Models for This RAG Pipeline

- `qwen3:14b` for best balance of quality and local speed.
- `gpt-oss:20b` for stronger reasoning quality if hardware allows.
- `deepseek-r1:14b` if you want stronger deliberate reasoning style.

For very limited hardware, `qwen3:8b` is the practical minimum. Avoid `tinyllama` for production-quality answers.

If you set a model that does not fit memory, backend will auto-retry smaller locally available models and return `model_used` in the API response.

## Ingestion Pipeline

`backend/ingestion.py` does:

1. Load GNEM Excel and clean column names
2. Parse city/county from location fields
3. Merge explicit company latitude/longitude from `GNEM - Auto Landscape Lat Long Updated File (1).xlsx` when present
4. Fall back to county centroid lat/lon from `Counties_Georgia.geojson` when explicit coordinates are unavailable
5. Write `companies` table to DuckDB
6. Build field-aware chunk records (improved chunking):
   - `company_profile`
   - `supply_chain`
   - `products_capabilities`
   - `geo_operations`
7. Write `company_chunks` table to DuckDB
8. Embed chunk texts and index in FAISS
9. Save chunk metadata JSON for citation tracing

Generated files:

- `data/gnem.duckdb`
- `data/gnem_faiss.index`
- `data/vector_metadata.json`

## API

Endpoint: `POST /chat`

Request:

```json
{
  "question": "Which EV suppliers are near Atlanta?"
}
```

Response includes:

- `answer` (LLM-generated from retrieved chunks)
- `sources` (chunk-level source lines)
- `retrieved_chunks` (full chunk metadata used for context)
- `retrieved_companies` (tabular view)
- `plan` (query planner output)
- `model_used` (active Ollama model)

## UI

`frontend/app.py` provides:

- Chat interface + conversation history
- Chunk-level source citations
- Detailed retrieved chunk table
- Supplier coordinate map with heatmap + point markers
- Retrieved companies table

Map colors indicate coordinate provenance:

- `coordinates_excel:*` = company-level coordinates from the uploaded lat/long workbook
- `source_excel` = coordinates found directly in the main GNEM sheet
- `county_centroid` = estimated county centroid fallback

## Notes

- This pipeline is LLM-only for final answers. If Ollama/model is unavailable, `/chat` returns an error.
- For best quality, use a stronger model than `tinyllama` (for example `qwen3:14b`).
