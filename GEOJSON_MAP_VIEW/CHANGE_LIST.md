# Final Change List

This is the final **code-side** change list required to make the project geometry-correct, reproducible, and demo-safe.

Assumption for this list:
- data mistakes are **not** being fixed here
- the code must either:
  - operate correctly on validated rows, or
  - fail fast when the geo layer is not trustworthy

Bottom line:
- the current code is **not ready**
- the items below are the minimum final code changes required for a research-grade build

## P0

### P0.1 Add a real geo-ingestion gate

File:
- `backend/ingestion.py`

Functions to change:
- `attach_coordinates`
- `run_ingestion`
- `load_county_centroids`

What to do:
- stop using county-centroid fallback as valid geo truth
- load the county GeoJSON as real polygons, not centroids
- standardize on a canonical county identifier:
  - prefer county FIPS if present in the GeoJSON
  - otherwise use a canonical normalized county name
- add `normalize_county_name()` and use it in ingestion, planner, spatial logic, analytics tables, and UI dropdowns
- sanitize county geometries at load time:
  - use `make_valid`
  - if needed, use a controlled fallback such as `buffer(0)`
- log whether any polygons required repair
- log polygon-repair provenance:
  - count of repaired polygons
  - list of affected counties
  - hash of the repaired geometry payload actually used by the run
- compute `computed_county` for every row with usable coordinates using point-in-polygon
- quarantine rows that are outside Georgia polygons or unassignable
- generate `geo_validation_report.csv`
- make the coordinate join audit a first-class ingestion gate before quarantine
- define gate metrics explicitly:
  - `join_match_rate` = percent of source rows that matched the coordinate enrichment join as intended
  - `duplicate_key_rate` = percent of join keys that are duplicated or ambiguous in the coordinate enrichment source
  - `outside_ga_rate` = percent of usable-coordinate points outside all Georgia county polygons
  - `unassignable_rate` = percent of usable-coordinate points with no assignable county
  - `mismatch_rate` = percent where `existing_county != computed_county` only when the existing county field is considered trustworthy
- fail ingestion when:
  - `join_match_rate` is below the configured threshold
  - `duplicate_key_rate` is above the configured threshold
  - `outside_ga_rate > 1%`
  - `unassignable_rate > 0.5%`
  - `mismatch_rate > 5%` to `10%` only when the existing county field is trusted
- recommended default thresholds for research-grade runs:
  - `join_match_rate < 99%` -> fail
  - `duplicate_key_rate > 0.5%` -> fail
- if the existing county field is not trusted, treat county mismatch as:
  - a warning and audit signal
  - not the sole fail-fast trigger
- define and log a configuration flag such as `COUNTY_FIELD_TRUSTED=true|false`
- include the value of `COUNTY_FIELD_TRUSTED` in every ingestion run log and audit artifact

Required new audit outputs:
- `coordinate_join_audit.csv` or `join_audit.json` containing:
  - join key(s)
  - match rate
  - duplicate keys and how they were resolved
  - sample mismatched joins with company identifier, company name, address, and lat/lon
- join key used
- join match rate
- duplicate key count
- examples of mismatched company/address/coordinate rows
- mismatch-rate summary

Why this is required:
- without this, the system can silently index bad geo rows and then produce invalid map, county, and distance results

Implementation note:
- replace `load_county_centroids` with polygon-loading helpers
- keep centroids only as an explicitly labeled fallback utility, never as the basis for county-distance logic

### P0.2 Replace centroid-based county reasoning with polygon-based geometry

File:
- `backend/spatial_engine.py`

Functions to change:
- `__init__`
- `_load_companies`
- `_build_city_centroids`
- `_resolve_city_coordinates`
- `companies_within_radius`
- `companies_near_city`

New functions to add:
- `companies_in_county(county_name: str)`
- `companies_within_miles_of_county(county_name: str, miles: float)`
- `compute_point_to_county_distance_miles(latitude: float, longitude: float, county_name: str)`
- projected-CRS helpers using `EPSG:5070`

What to do:
- load county polygons once at engine startup
- standardize county-distance math on `EPSG:5070`
- `EPSG:5070` is the standard CRS for this project; distances are approximate planar distances suitable at county scale
- enforce `EPSG:5070` consistently in:
  - county-distance computation
  - geo tests
  - geo evidence payloads
  - logs
- define county distance as:
  - `0` if point is inside polygon
  - else minimum projected distance from point to polygon boundary
- keep point-radius search geodesic for `within N miles of a point`
- remove live `Nominatim` fallback from `_resolve_city_coordinates`

Why this is required:
- current county logic is geometry-wrong
- centroid distance is not acceptable for county queries

### P0.3 Add explicit county and gap-query planning

File:
- `backend/query_planner.py`

Functions to change:
- `plan`
- `_extract_radius_km`
- `_extract_city`
- add county extractors and gap-query parsing helpers

What to do:
- parse:
  - `in <county>`
  - `within <N> miles of <county>`
  - `within <N> miles of <lat,lon>`
  - `counties with 0 <role/category>`
- add explicit route types:
  - `lookup`
  - `analytic_local`
  - `llm_synthesis`
  - `web_needed`
- add structured plan fields:
  - `target_county`
  - `radius_miles`
  - `geo_anchor_type`
  - `analytic_metric`
  - `requires_polygon_distance`

Why this is required:
- the current planner is too heuristic and does not expose a research-auditable route decision

### P0.4 Route geo questions to deterministic paths before LLM narration

File:
- `backend/rag_pipeline.py`

Functions to change:
- `answer_question`
- `_run_sql_retrieval`
- `_apply_structured_filters`
- `_build_geo_no_results_chunk`
- `_build_retrieved_chunks`
- `_format_context`
- `_generate_answer_with_llm`

New helpers to add:
- deterministic route handlers for:
  - county membership
  - county-distance queries
  - point-radius queries
  - zero-gap analytics
- citation validation helper

What to do:
- run geometry or analytic computation first
- only let `llm_synthesis` narrate results that already exist as deterministic evidence
- allow `llm_synthesis` only if at least one deterministic evidence source exists:
  - non-empty `GEO` evidence and/or
  - non-empty `ANALYTIC` evidence
- if deterministic geo/analytic evidence is empty:
  - abstain, or
  - return an explicit `not found` style response
- never let the LLM invent:
  - which companies are in a county
  - which companies are within N miles of a county
  - which counties have zero role/category matches

Why this is required:
- geo-aware RAG must be geometry-backed, not semantic-search-backed

### P0.5 Replace generic citations with audit-friendly evidence IDs

File:
- `backend/rag_pipeline.py`

Functions to change:
- `_build_retrieved_chunks`
- `_chunk_source_line`
- `_format_context`
- `_generate_answer_with_llm`

What to do:
- replace `[C1]` style citations with:
  - `DOC:<chunk_id>`
  - `GEO:<operation>|county=<county>|company=<slug>`
  - `ANALYTIC:<table>|<metric>|<group>`
- keep the `GEO:` identifier stable
- always log a structured `geo_evidence` object alongside it:
  - `operation`
  - `county`
  - `company_id` or stable company slug
  - `dist_mi`
  - `crs`
  - `method`
- treat computed values such as `dist_mi` and `method` as structured query-time metadata, not as information that exists only inside the citation string
- pass evidence IDs into response payloads and UI

Why this is required:
- citations must show where a geo claim came from and what was computed

### P0.6 Add post-generation citation enforcement

File:
- `backend/rag_pipeline.py`

Functions to change:
- `_generate_answer_with_llm`
- add a new validation helper after generation

What to do:
- inspect every non-abstaining bullet in the final answer
- require at least one citation token:
  - `[DOC:...]`
  - `[GEO:...]`
  - `[ANALYTIC:...]`
- default policy:
  - Eval mode -> abstain with explanation
  - UI mode -> deterministic fallback summary
- use `MODE=eval` as the default for benchmark and experiment runs
- make the mode explicit in configuration and logs so runs are reproducible

Why this is required:
- unsupported geo claims should never appear in the final answer

### P0.7 Add true county polygons and reliable county selection to the UI

File:
- `frontend/app.py`

Functions to change:
- `call_backend`
- `render_sources`
- `render_chunks`
- `render_table`
- `render_map`
- `start_backend_on_port`

What to do:
- current frontend stack is `pydeck`, so render a `GeoJsonLayer` or `PolygonLayer` for Georgia counties
- lock this implementation plan to `pydeck` for the current build
- add county dropdown selection in the sidebar
- highlight selected county
- keep polygon click optional, not required
- add county tooltip content:
  - county name
  - total count
  - filtered count
  - role/category summary
- keep company points visible with tooltips including:
  - name
  - role/category
  - county
  - latitude
  - longitude

Why this is required:
- the frontend currently shows points only, not a real county GeoJSON map

## P1

### P1.1 Add deterministic analytic tables in DuckDB

Files:
- `backend/ingestion.py`
- new `backend/analytics_engine.py`
- optionally `backend/sql_engine.py`

What to build:
- `county_company_counts`
- `county_role_counts`
- `county_category_counts`
- zero-gap support tables or views

Why this is required:
- gap queries and county summaries should come from tables, not from fuzzy retrieval

### P1.2 Make embeddings deterministic and explicit

Files:
- `backend/ingestion.py`
- `backend/vector_engine.py`

Functions to change:
- `create_embeddings`
- `_init_embedder`
- `_embed_query`
- metadata loading/writing paths

What to do:
- remove silent hash fallback as an invisible behavior
- require one explicit embedding mode for both indexing and query time
- write embedding backend + model name into metadata
- refuse to load mismatched index/backend combinations

Why this is required:
- silent embedding fallback breaks reproducibility

### P1.3 Pin dependencies and add missing geometry/test stack

File:
- `requirements.txt`

What to do:
- pin versions for all current packages
- add and pin:
  - `shapely`
  - `pyproj`
  - `pytest`
  - `openpyxl`
- do not add alternate mapping-stack dependencies in the current `pydeck` build

Why this is required:
- a research-grade build must be reproducible across machines

### P1.4 Add structured JSONL logging

Files:
- `backend/main.py`
- new `backend/logging_utils.py`

Functions to change:
- `startup_event`
- `chat`

What to log:
- timestamp
- user query
- planner route
- county/radius/anchor
- evidence IDs
- configured CRS
- `COUNTY_FIELD_TRUSTED`
- retrieval summary
- selected model
- embedding backend
- git commit hash
- GeoJSON file hash
- Excel file hash or last-modified timestamp
- answer text
- errors

Why this is required:
- you need an auditable trail for demo and evaluation

### P1.5 Add a minimal pytest suite

New directory:
- `tests/`

Required tests:
- planner parses county-radius query correctly
- point-in-polygon assigns known county correctly
- projected county-distance query returns expected ordering or threshold behavior
- gap analytics returns deterministic result
- citation validator rejects uncited bullets
- ingestion gate fails when thresholds are exceeded

Why this is required:
- currently there is no protection against regression in the geo pipeline

### P1.6 Add a deterministic geo truth slice

Files:
- new `tests/data/`
- new evaluation helper module if needed

What to include:
- county assignment truth cases
- county-distance truth cases
- zero-gap truth cases

Why this is required:
- this gives you a professor-defensible evaluation slice independent of web search

## P2

### P2.1 Improve the county UI for demo quality

File:
- `frontend/app.py`

What to add:
- county choropleth shading
- county summary side panel
- clear display of geo method:
  - polygon containment
  - polygon distance in `EPSG:5070`
  - geodesic point radius

### P2.2 Improve backend startup and error visibility

Files:
- `frontend/app.py`
- `backend/main.py`

What to do:
- stop swallowing backend startup errors
- surface model, ingestion, and geometry initialization failures clearly in the UI

### P2.3 Separate research evidence from UX text

Files:
- `frontend/app.py`
- `backend/rag_pipeline.py`

What to add:
- evidence panel
- route-type display
- optional debug panel showing:
  - deterministic geo result
  - analytic table hits
  - final narrated answer

## New Files Recommended

- `backend/analytics_engine.py`
- `backend/logging_utils.py`
- `tests/test_query_planner.py`
- `tests/test_spatial_engine.py`
- `tests/test_analytics_engine.py`
- `tests/test_rag_citations.py`
- `tests/test_ingestion_gate.py`

## Final Build Definition

The code should be considered ready only when all of the following are true:

- county polygons are rendered in the UI
- county dropdown selection works
- county assignment is point-in-polygon
- county-distance uses projected polygon distance in `EPSG:5070`
- point-radius uses geodesic distance
- county and gap queries route to deterministic local paths
- answers use `DOC:`, `GEO:`, and `ANALYTIC:` evidence IDs
- uncited bullets are blocked
- dependencies are pinned
- embeddings are deterministic
- ingestion fails fast on geo-integrity failure
- JSONL logging exists
- pytest coverage exists for the geo-critical path

## Final Recommendation

If you want the codebase to work correctly after the data is repaired or quarantined, implement the work in this order:

1. `backend/ingestion.py`
2. `backend/spatial_engine.py`
3. `backend/query_planner.py`
4. `backend/rag_pipeline.py`
5. `frontend/app.py`
6. `backend/main.py`
7. `requirements.txt`
8. `tests/`

That is the final code change set required.
