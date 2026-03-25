# Change List

## P0

- Add a coordinate-join audit in `backend/ingestion.py` before quarantine:
  - join key used
  - match rate
  - duplicate key count
  - examples of mismatched company/address/coordinate pairs
- Add the P0.0 geo correctness gate in `backend/ingestion.py`: compute `computed_county` via point-in-polygon, quarantine invalid rows, write `geo_validation_report.csv` plus mismatch-rate summary, and fail ingestion if:
  - `outside_ga_rate > 1%`
  - `county_mismatch_rate > 5%` to `10%`
- Remove all centroid-based county distance logic from `backend/spatial_engine.py`; implement county containment and point-to-county-polygon minimum distance in `EPSG:5070`, then convert meters to miles.
- Add county polygon overlay, county tooltip counts, county dropdown, and county highlight state to `frontend/app.py`.
- Extend `backend/query_planner.py` to parse:
  - `in <county>`
  - `within <N> miles of <county>`
  - `counties with 0 <role/category>`
- Add explicit route transparency in the planner/pipeline: `lookup`, `analytic_local`, `llm_synthesis`, `web_needed`.
- Route county/gap questions to deterministic local geometry or analytic paths in `backend/rag_pipeline.py`, and allow `llm_synthesis` only after deterministic evidence has already been computed.
- Replace generic `[C1]` citations with stable `DOC:`, `GEO:`, and `ANALYTIC:` evidence ids in `backend/rag_pipeline.py`; make `GEO:` ids carry the computed value and CRS, for example `GEO:within_miles_of_county|county=cobb|company=xyz|dist_mi=12.4|crs=EPSG:5070`.
- Add a post-generation citation validator in `backend/rag_pipeline.py` so every non-abstaining bullet must carry a citation token.

## P1

- Materialize county analytics tables in DuckDB for:
  - company count per county
  - role/category distributions per county
  - zero-count gap queries
- Pin dependencies in `requirements.txt` or a lockfile, including geometry and test packages.
- Remove silent embedding fallback from `backend/ingestion.py` and `backend/vector_engine.py`; fail fast or rebuild consistently with an explicit deterministic fallback mode.
- Remove live `Nominatim` geocoder fallback from `backend/spatial_engine.py`.
- Add `tests/` with at least five pytest cases:
  1. parse radius + county intent
  2. point-in-polygon county assignment
  3. projected-CRS polygon-distance query
  4. deterministic gap analytics
  5. citation enforcement
- Add JSONL logging for query, route/plan, selected county/radius, evidence ids, retrieval summary, model, embedding backend, answer, and errors.
- Add a deterministic geo truth slice for evaluation: county assignment, polygon-distance county queries, and zero-count gap queries.

## P2

- Add county choropleth shading and county summary side panel in the frontend.
- Surface the geo method in the UI: polygon containment, polygon distance in projected CRS, and geodesic point radius.
- Improve backend startup diagnostics instead of swallowing backend stdout/stderr in the frontend.
