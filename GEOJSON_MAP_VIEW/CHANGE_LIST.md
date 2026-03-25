# Change List

## P0

- Add county `GeoJsonLayer` overlay, county tooltip counts, and county dropdown to `frontend/app.py`.
- Replace centroid-based county logic in `backend/spatial_engine.py` with polygon-aware containment and point-to-polygon distance.
- Extend `backend/query_planner.py` to parse county intents and gap queries explicitly.
- Route county/gap queries to deterministic geo/analytic paths in `backend/rag_pipeline.py`.
- Validate coordinates against `Counties_Georgia.geojson` during ingestion and quarantine bad rows in `backend/ingestion.py`.
- Replace generic `[C1]` citations with stable `DOC:`, `GEO:`, and `ANALYTIC:` evidence ids in `backend/rag_pipeline.py`.

## P1

- Materialize county analytics tables in DuckDB for counts, role/category distributions, and zero-count gap queries.
- Pin all dependencies in `requirements.txt` and add geometry/test packages.
- Remove silent embedding fallback in `backend/ingestion.py` and `backend/vector_engine.py`.
- Remove live `Nominatim` geocoder fallback from `backend/spatial_engine.py`.
- Add `tests/` with at least five pytest cases covering geo parsing, polygon distance, gap analytics, and citation enforcement.
- Add JSONL logging for query, plan, retrieval summary, model, embedding backend, answer, and errors.

## P2

- Add county choropleth shading and county summary side panel in the frontend.
- Surface the geo method in the UI: polygon containment, polygon distance, geodesic point radius.
- Improve backend startup diagnostics instead of swallowing backend stdout/stderr in the frontend.
