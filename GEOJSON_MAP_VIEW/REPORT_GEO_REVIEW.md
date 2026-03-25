# GEOJSON Map + Geo-Aware RAG Review

## Scope

This review is based on:

- Static inspection of the full codebase
- Targeted runtime probes of the planner, spatial engine, DuckDB contents, vector metadata, and coordinate artifacts
- Targeted data validation against the Georgia county GeoJSON

This is enough to evaluate correctness, fragility, reproducibility, and Summit readiness. It is not a UX/browser usability test.

## Executive Verdict

Short version: the repository has the **right high-level architecture**, but it does **not yet meet the full “GeoJSON + map view + geo-aware RAG queries” requirement set**.

What is working:

- A Streamlit UI exists
- A FastAPI backend exists
- Company points can be shown on a map with tooltips
- Point-radius filtering exists
- SQL, vector, and geo retrieval are wired together
- LLM answers include generic chunk citations

What is not working at the required rigor level:

- The map does **not** visibly render county GeoJSON polygons
- County-based geospatial reasoning is **centroid-based**, not polygon-based
- County assignment is **not** computed by point-in-polygon
- Gap/density/county analytic queries are **not** implemented as deterministic analytics
- The coordinate source appears materially unreliable and is not validated against county polygons
- Dependencies are unpinned, embeddings default to hash fallback, and runtime embedding fallback is silent
- There is no test suite and no structured query/output logging

Bottom line: **PARTIALLY MEETS the intended demo concept, but does not yet meet the geometry-correct, research-grade, professor-defensible implementation bar.**

## A) Repo Scan

### Main Tree / Entrypoints

```text
GEOJSON_MAP_VIEW/
├── README.md
├── requirements.txt
├── frontend/
│   └── app.py
├── backend/
│   ├── ingestion.py
│   ├── main.py
│   ├── query_planner.py
│   ├── rag_pipeline.py
│   ├── spatial_engine.py
│   ├── sql_engine.py
│   └── vector_engine.py
└── data/
    ├── Counties_Georgia.geojson
    ├── gnem_companies.xlsx
    ├── GNEM - Auto Landscape Lat Long Updated File (1).xlsx
    ├── gnem.duckdb
    ├── gnem_faiss.index
    └── vector_metadata.json
```

### Architecture Summary

- `frontend/app.py`
  - Streamlit UI
  - Uses `pydeck`
  - Renders only a heatmap + scatterplot company points
  - Auto-starts the backend if needed

- `backend/main.py`
  - FastAPI app
  - Startup loads `HybridGeospatialRAGPipeline`
  - `POST /chat` returns answer, sources, retrieved chunks/companies, plan, model

- `backend/ingestion.py`
  - Loads Excel
  - Parses city/county from text
  - Enriches coordinates from the external lat/lon workbook
  - Falls back to county centroids
  - Writes `companies` and `company_chunks` to DuckDB
  - Builds FAISS index and vector metadata

- `backend/sql_engine.py`
  - Simple DuckDB filters for OEM, industry, employment, and string search

- `backend/spatial_engine.py`
  - Loads company rows from DuckDB
  - Builds city “centroids” from company point averages
  - Loads county centroids from the GeoJSON
  - Supports point-radius filtering only

- `backend/vector_engine.py`
  - Loads FAISS and vector metadata
  - Uses sentence-transformers if available
  - Silently falls back to hash embeddings otherwise

- `backend/rag_pipeline.py`
  - Orchestrates planner + retrieval
  - Builds retrieved chunks
  - Calls Ollama through the OpenAI-compatible API
  - Returns generic `[C1]`, `[C2]`-style citations

### Mapping Library

- Library used: `pydeck` in `frontend/app.py`
- Current layers:
  - `HeatmapLayer`
  - `ScatterplotLayer`
- No `GeoJsonLayer` or `PolygonLayer` is used
- No polygon-selection path exists in the code

Note on interaction:

- In this codebase, `st.pydeck_chart(...)` is used only for rendering and hover tooltips.
- There is no implemented path for polygon click state to flow back into Python.
- For Summit, a county dropdown is the safest immediate interaction model unless the app adopts a different map component or a custom event bridge.

### Where Company Data Is Loaded and County Info Is Assigned

- Excel load: `backend/ingestion.py::run_ingestion`
- County parsing: `backend/ingestion.py::extract_city_county`
- County assignment in stored rows:
  - parsed from `Location`
  - optionally overridden from coordinate workbook columns
  - **not** computed from point-in-polygon

### Where Geo Computations Happen

- County centroid extraction: `backend/ingestion.py::load_county_centroids`
- Point-radius filtering: `backend/spatial_engine.py::companies_within_radius`
- “Near city/county” resolution:
  - `backend/spatial_engine.py::_resolve_city_coordinates`
  - `backend/spatial_engine.py::companies_near_city`

### Where Retrieval / LLM Integration Happens

- Planner: `backend/query_planner.py::plan`
- SQL retrieval: `backend/sql_engine.py`
- Vector retrieval: `backend/vector_engine.py::semantic_company_search`
- Geo retrieval merge/orchestration: `backend/rag_pipeline.py::answer_question`
- Context assembly: `backend/rag_pipeline.py::_build_retrieved_chunks` and `_format_context`
- Citation formatting: `backend/rag_pipeline.py::_chunk_source_line`
- LLM call: `backend/rag_pipeline.py::_generate_answer_with_llm`

## Key Runtime Findings

### 1) The shipped map is not a GeoJSON county map

`frontend/app.py::render_map` renders only company points and a heatmap. The county GeoJSON file is never loaded in the UI. This means the app is not currently a visible GeoJSON polygon viewer.

### 2) County geometry is reduced to centroids

`backend/ingestion.py::load_county_centroids` converts each county polygon into the mean of all polygon vertices. `backend/spatial_engine.py` then uses these centroids to answer county-like proximity queries.

That is not acceptable for:

- “companies in county X”
- “within N miles of county X”
- county gap analysis

### 3) Query planner does not support county semantics correctly

Observed planner outputs:

- `"show companies in Fulton County"` -> `VECTOR_QUERY`, no geo hint
- `"within 50 miles of Fulton County"` -> `GEO_QUERY` with `city='Fulton County'`
- `"which counties have 0 battery companies"` -> `HYBRID_QUERY` with `capability_term='battery'`, not a county-zero analytic

This means requirement A and D are not implemented as explicit geo/analytic intents.

### 4) Coordinate integrity is the largest correctness risk

Observed from `gnem.duckdb`:

- 207 company rows total
- 202 rows use the external coordinate workbook
- 4 rows use county centroid fallback
- 1 row is still missing coordinates

Observed from `vector_metadata.json` and DuckDB probes:

- The current built artifact stores many exact workbook coordinates that are inconsistent with the labeled location/county.

Concrete examples from `GNEM - Auto Landscape Lat Long Updated File (1).xlsx`:

- `ACM Georgia LLC`
  - labeled location: `Calhoun, Gordon County`
  - address: `975 Thomson Hwy, Warrenton, GA 30828`
- `Adient`
  - labeled location: `Ringgold, Catoosa County`
  - address: `1700 S Progress Pkwy, West Point, GA 31833`
- `Elan Technology Inc.`
  - labeled location: `Atlanta, Fulton County`
  - address: `169 Elan Court, Midway, GA 31320`
- `Michelin Tread Technologies`
  - labeled location: `Lawrenceville, Gwinnett County`
  - address: `1 Parkway South Blvd, Greenville, SC 29615`

Polygon validation against `Counties_Georgia.geojson` found:

- `9` company points lie outside **all** Georgia county polygons
- `167` of `183` rows that have both a county label and coordinates do **not** fall inside their labeled county polygon

That is a Summit-blocking geo correctness issue.

### 5) City anchors are not true city anchors

`backend/spatial_engine.py::_build_city_centroids` averages company coordinates by city label. That means a query like “near Atlanta” uses the mean of stored company points labeled Atlanta, not a city boundary, city center, or authoritative gazetteer.

Runtime probe examples:

- `Atlanta -> (33.4244, -84.1040)`
- `Savannah -> (32.4205, -83.5304)`

The Savannah anchor is especially revealing: it is materially inland and not a defensible “Savannah” anchor.

### 6) Embedding pipeline is not research-grade reproducible

The current artifact reports:

- `embedding_backend: hash-fallback`
- `embedding_model: hashed-token-384`

That means the shipped vector index is not using a semantic embedding model at all.

The code also has two reproducibility problems:

- `backend/ingestion.py::create_embeddings`
  - defaults to hash fallback unless `EMBEDDING_BACKEND` is explicitly set
- `backend/vector_engine.py::_init_embedder`
  - silently falls back to hash query embeddings if the sentence-transformer cannot load

That silent runtime fallback is dangerous because it can produce **index/query embedding mismatch** if the FAISS index was built with one backend and queries are embedded with another.

## B) Requirements Checklist

| Requirement | Status | Evidence | Assessment |
| --- | --- | --- | --- |
| 1. County polygon overlay visible on map | MISSING | `frontend/app.py::render_map` | Only `HeatmapLayer` + `ScatterplotLayer`; no GeoJSON polygon layer is rendered. |
| 2. Company point layer visible with tooltips | MET | `frontend/app.py::render_map` | Company points are visible, pickable, and include tooltips. |
| 3. Choropleth shading by county counts | MISSING | no implementation found | No county aggregation and no polygon fill logic exist. |
| 4. County selection interaction (dropdown and/or polygon click) | MISSING | `frontend/app.py` sidebar + map | No county dropdown, no polygon click handling, no county highlight workflow. |
| 5. Correct county assignment (point-in-polygon) from GeoJSON | MISSING | `backend/ingestion.py::attach_coordinates` | County is parsed from text/workbook columns, not geometry. No spatial join is performed. |
| 6. Correct distance-to-county queries (min distance to polygon boundary) | MISSING | `backend/ingestion.py::load_county_centroids`, `backend/spatial_engine.py::companies_near_city` | County queries are routed through centroid-like points, not polygon boundary distance. |
| 7. Correct radius-from-point queries (haversine/geodesic) | PARTIALLY MET | `backend/spatial_engine.py::companies_within_radius` | Uses `geopy.distance`, but the underlying stored company coordinates are often invalid. |
| 8. Derived analytic summaries available (county counts, role distribution, gaps) | MISSING | no analytic table/module found | No deterministic county analytics or zero-count gap tables exist. |
| 9. Evidence-backed answers with strict citation format for geo evidence | PARTIALLY MET | `backend/rag_pipeline.py::_build_retrieved_chunks`, `_chunk_source_line`, `_generate_answer_with_llm` | Generic `[C1]` citations exist, but there is no stable `[GEO:...]` scheme, no geo evidence contract, and no bullet-level geo citation enforcement. |
| 10. Reproducibility: pinned dependencies + deterministic embedding model loading | MISSING | `requirements.txt`, `backend/ingestion.py::create_embeddings`, `backend/vector_engine.py::_init_embedder` | Dependencies are unpinned; embedding fallback is hash-based and silent. |
| 11. Minimal test suite present (pytest) | MISSING | no `tests/` directory or `pytest` use found | There are no automated tests. |
| 12. Logging of user queries and outputs (JSONL preferred) | MISSING | `backend/main.py` | No structured query/result logging exists. |

## C) What Is Missing / Incorrect / Fragile / Non-Reproducible

### P0 Correctness Problems

1. **The app is not visibly using GeoJSON polygons in the map UI.**
   - This alone means the current UI does not satisfy the “GeoJSON + map view” requirement as stated.

2. **County semantics are geometry-wrong.**
   - County queries use centroids, not polygons.
   - Distance-to-county is point-to-centroid, not point-to-polygon-boundary.

3. **County assignment is text-based, not geometric.**
   - The stored `county` field is not validated against coordinates.

4. **Coordinate quality is poor enough to invalidate many geo results.**
   - 9 points outside all Georgia counties
   - 167/183 county-labeled points outside their labeled county polygon

5. **City anchors are derived from noisy company averages.**
   - “near Atlanta” and “near Savannah” are not anchored to authoritative city geometry or even stable city centers.

### P1 Fragility / Reproducibility Problems

6. **The retrieval stack is not truly deterministic in a research sense.**
   - Unpinned dependencies
   - Embedding backend changes based on environment
   - Silent runtime fallback behavior

7. **The shipped vector index is currently hash-based.**
   - That is deterministic, but semantically weak and inconsistent with the expected FAISS + sentence-transformers story.

8. **Nominatim geocoder fallback introduces network dependence.**
   - `backend/spatial_engine.py::_resolve_city_coordinates` can call a live external geocoder.
   - That is non-reproducible and unsuitable for a research demo pipeline.

9. **Generic per-query chunk IDs are not stable citations.**
   - `C1`, `C2`, ... depend on retrieval order and are not durable evidence identifiers.

10. **No tests, no logs, no experiment record.**
   - This makes professor-level scrutiny difficult to answer.

## D) Prioritized Correction Plan

## P0: Must Fix Before Summit / Professor Review

### P0.1 Replace centroid county logic with polygon-aware geometry

- What to change:
  - Implement true county polygon loading and geometry operations.
- Where:
  - `backend/spatial_engine.py`
  - `backend/query_planner.py`
  - `backend/rag_pipeline.py`
- Why it matters:
  - This is the core requirement for county-aware RAG.
  - Current centroid logic is not defensible for county distance or containment questions.
- How to implement:
  - Add `shapely` and `pyproj`
  - Load county polygons once from `Counties_Georgia.geojson`
  - Store:
    - county polygon geometry
    - county display properties
  - Add functions:
    - `companies_in_county(county_name)`
    - `companies_within_distance_of_county(county_name, radius_miles)`
    - `companies_within_distance_of_point(lat, lon, radius_miles)`
    - `counties_with_zero_companies(filters)`
  - Use:
    - point-in-polygon for county inclusion
    - polygon-to-point minimum distance for “within N miles of county X”
  - Keep centroid only as a clearly labeled fallback when coordinates are missing.

### P0.2 Validate coordinates during ingestion and quarantine bad rows

- What to change:
  - Add ingestion-time geo validation against the county polygons.
- Where:
  - `backend/ingestion.py::attach_coordinates`
  - `backend/ingestion.py::run_ingestion`
  - data workbook curation process
- Why it matters:
  - Right now the map and geo retrieval are operating on many invalid coordinates.
- How to implement:
  - Load county polygons during ingestion
  - For each row with coordinates:
    - verify point is inside Georgia
    - if county is known, verify point is inside labeled county
  - Add columns:
    - `geo_validation_status`
    - `validated_county`
    - `geo_validation_note`
  - Write a validation artifact such as `data/geo_validation_report.jsonl`
  - Fail ingestion if invalid-rate exceeds a threshold
  - Review and repair the external coordinate workbook before rebuilding artifacts

### P0.3 Add visible GeoJSON polygon overlay and county-focused UI

- What to change:
  - Render county polygons on the map and expose county selection.
- Where:
  - `frontend/app.py`
- Why it matters:
  - The current UI is a point map, not a county GeoJSON map.
- How to implement:
  - Load `Counties_Georgia.geojson` in the frontend
  - Add a `GeoJsonLayer` or `PolygonLayer`
  - Add county tooltip fields:
    - county name
    - total company count
    - filtered company count
    - role/category count
  - Add a sidebar county dropdown immediately
  - Highlight selected county polygon
  - Add optional choropleth fill based on company counts

### P0.4 Implement deterministic county analytics for gaps and density

- What to change:
  - Add an analytic layer instead of trying to answer county gap queries via semantic retrieval.
- Where:
  - New module recommended: `backend/analytics_engine.py`
  - Or extend `backend/sql_engine.py` plus `backend/rag_pipeline.py`
- Why it matters:
  - “Which counties have 0 battery companies?” is an analytic query, not a vector retrieval question.
- How to implement:
  - Build deterministic county summary tables:
    - `county_company_counts`
    - `county_role_counts`
    - `county_category_counts`
  - Join company points to county polygons
  - Materialize per-county counts in DuckDB during ingestion
  - Add pipeline support for:
    - top counties by count
    - zero-count counties
    - density by county
    - role/category distribution

### P0.5 Enforce stable geo citations

- What to change:
  - Replace generic query-local `C1` citations with stable geo/doc identifiers.
- Where:
  - `backend/rag_pipeline.py::_build_retrieved_chunks`
  - `backend/rag_pipeline.py::_format_context`
  - `backend/rag_pipeline.py::_chunk_source_line`
  - LLM prompt in `backend/rag_pipeline.py::_generate_answer_with_llm`
- Why it matters:
  - Professor-level scrutiny will ask what exact evidence supports the geo answer.
- How to implement:
  - Emit stable IDs such as:
    - `DOC:<chunk_id>`
    - `GEO:county=<county>:company=<slug>`
    - `ANALYTIC:county=<county>:metric=<metric>`
  - For geo answers, require at least one `GEO:` or `ANALYTIC:` citation
  - Prefer bullet-level evidence statements with one citation per bullet

## P1: Reproducibility / Research-Grade Hardening

### P1.1 Pin dependencies and record runtime versions

- What to change:
  - Replace loose requirements with pinned versions.
- Where:
  - `requirements.txt`
  - add lockfile such as `requirements-lock.txt`
- Why it matters:
  - Current installs are non-reproducible.
- How to implement:
  - Pin every package version
  - Record Python version in README
  - Add `shapely`, `pyproj`, and `pytest`
  - Consider a compiled lock via `pip-compile`

### P1.2 Remove silent embedding fallbacks

- What to change:
  - Make embedding backend explicit and fail-fast.
- Where:
  - `backend/ingestion.py::create_embeddings`
  - `backend/vector_engine.py::_init_embedder`
- Why it matters:
  - Silent fallback can produce invalid retrieval behavior and undermines reproducibility.
- How to implement:
  - Require explicit `EMBEDDING_BACKEND`
  - Default to sentence-transformers, not hash
  - If configured model is unavailable, fail ingestion/startup unless an explicit override is set
  - Log embedding backend/model into startup and query logs

### P1.3 Remove live geocoder fallback from production path

- What to change:
  - Eliminate runtime `Nominatim` dependency.
- Where:
  - `backend/spatial_engine.py::_resolve_city_coordinates`
- Why it matters:
  - Network calls make results non-reproducible.
- How to implement:
  - Replace with local gazetteer or fixed county/city anchor table
  - Restrict supported anchor types to:
    - county polygon
    - county centroid fallback
    - explicit point
    - curated city centroid table

### P1.4 Add minimal pytest suite

- What to change:
  - Add a `tests/` directory with at least five tests.
- Where:
  - new `tests/` package
- Why it matters:
  - There is currently zero automated assurance.
- Minimum tests:
  1. GeoJSON polygon load + county lookup
  2. Point-in-polygon county assignment
  3. Distance-to-county uses polygon boundary, not centroid
  4. Point-radius query returns expected companies
  5. Geo/county gap analytic returns zero-count counties deterministically
  6. Geo answers require stable geo citations

### P1.5 Add JSONL logging

- What to change:
  - Log queries, plan, retrieval summary, answer, model, and embedding backend.
- Where:
  - `backend/main.py`
  - recommended new helper: `backend/logging_utils.py`
- Why it matters:
  - Needed for reproducibility, debugging, and demo postmortems.
- How to implement:
  - Append one JSON object per request to `logs/chat_runs.jsonl`
  - Include:
    - timestamp
    - question
    - plan
    - model
    - embedding backend/model
    - retrieved company ids
    - citations returned
    - errors if any

## P2: Demo Polish / UX Hardening

### P2.1 Add county choropleth and summary panel

- Where:
  - `frontend/app.py`
- Why:
  - Makes county gaps and density instantly visible during Summit demo.

### P2.2 Add explicit “geo method” disclosure in UI

- Where:
  - `frontend/app.py`
- Why:
  - Helpful for professor questions.
- Show:
  - “county containment = polygon”
  - “county distance = min point-to-polygon distance”
  - “point radius = geodesic distance”

### P2.3 Improve backend startup/debugging UX

- Where:
  - `frontend/app.py`
- Why:
  - Current auto-start hides stdout/stderr, which slows diagnosis during demos.

## Concrete File/Function Edit Targets

### Highest-value edits

- `frontend/app.py::render_map`
  - add county GeoJSON layer, selected county highlight, choropleth

- `backend/spatial_engine.py`
  - replace centroid-only county logic with polygon operations
  - remove live geocoder fallback

- `backend/query_planner.py::plan`
  - add explicit parsing for:
    - `in <county>`
    - `within N miles of <county>`
    - `within N miles of <lat, lon>`
    - `which counties have 0 ...`

- `backend/rag_pipeline.py::answer_question`
  - route county and gap queries to deterministic geo/analytic functions

- `backend/rag_pipeline.py::_build_retrieved_chunks`
  - emit stable `GEO:` and `ANALYTIC:` evidence ids

- `backend/ingestion.py::attach_coordinates`
  - validate coordinates against county polygons
  - stop accepting invalid rows silently

- `backend/ingestion.py::create_embeddings`
  - fail-fast on embedding model issues unless explicitly overridden

- `backend/vector_engine.py::_init_embedder`
  - remove silent runtime hash fallback

- `requirements.txt`
  - pin versions and add geometry/test dependencies

## Recommended Summit Demo Workflow

Use this flow once the P0 items are fixed:

1. Start on a county polygon map of Georgia
   - county polygons visible
   - choropleth = total supplier count

2. Select a county from a dropdown
   - polygon highlights
   - side panel shows:
     - total companies
     - top roles
     - top OEM links

3. Ask:
   - “Show companies in Fulton County”
   - map shows in-county points only
   - response cites `GEO:` evidence items

4. Ask:
   - “Which battery suppliers are within 50 miles of Fulton County?”
   - system uses polygon boundary distance
   - response shows exact distance method and citations

5. Ask:
   - “Which counties have 0 battery companies?”
   - county gap choropleth highlights zero-count counties
   - response cites deterministic `ANALYTIC:` evidence

6. Open evidence panel
   - show document citations
   - show geo citations
   - show analytics table row ids

## Final Assessment

If judged as a prototype:

- the project is coherent and promising

If judged as a geometry-correct, research-grade geospatial RAG system:

- it is **not yet there**

The biggest blockers are:

1. no visible county polygon map
2. centroid-based county reasoning
3. coordinate integrity problems
4. missing deterministic county analytics
5. missing reproducibility discipline

Those are all fixable, but they must be fixed before claiming that the system truly supports GeoJSON-based county reasoning.
