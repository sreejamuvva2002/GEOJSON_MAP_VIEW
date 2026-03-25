# GEOJSON + Map View + Geo-Aware RAG Review

## Scope

This review is based on:

- Full static inspection of the repository
- Targeted runtime probes of the planner, spatial engine, DuckDB artifacts, vector metadata, and coordinate sources
- Polygon validation of stored company points against `data/Counties_Georgia.geojson`

This is enough to judge correctness, fragility, reproducibility, and Summit readiness. It is not a full browser interaction test.

## Executive Verdict

The repository has a solid prototype skeleton, but it does **not** currently satisfy the professor-defensible version of:

- GeoJSON county map view
- polygon-correct county math
- geo-aware RAG routing
- reproducible research pipeline

Current state in one sentence:

**The codebase partially demonstrates a geospatial RAG prototype, but it fails the geo-correctness gate because county polygons are not rendered in the UI, county reasoning is centroid-based rather than polygon-based, and the stored coordinates are not validated before indexing or mapping.**

Plain-language status:

**Everything is not ok yet for a professor-defensible Summit demo.**
The prototype is promising, but the geo layer should still be treated as unsafe until the coordinate join is audited, the fail-fast thresholds are enforced, and county math is rebuilt on true polygons.

## Bottom-Line Assessment

### What is already present

- Streamlit frontend
- FastAPI backend
- SQL + vector + geo retrieval modules
- Ollama-backed answer synthesis
- Company point visualization with hover tooltips
- Point-radius filtering via `geopy`

### What is not yet professor-defensible

- No visible county GeoJSON polygon overlay in the frontend
- No county dropdown interaction
- No point-in-polygon county assignment
- No projected-CRS point-to-polygon county distance
- No deterministic county analytics for zero-count gap questions
- No transparent route types such as `lookup`, `analytic_local`, `web_needed`
- No stable `DOC:`, `GEO:`, `ANALYTIC:` evidence IDs
- No post-generation citation validator
- No pinned dependencies
- No test suite
- No JSONL query/evidence logging

## P0.0 Geo Correctness Gate

### Required Gate Definition

Before any UI or RAG claims are trusted, the pipeline must:

1. Audit the coordinate join before quarantine:
   - join key used
   - match rate
   - duplicate key count
   - examples of mismatched company/address/coordinate pairs
2. Compute `computed_county` for every company point using point-in-polygon against `Counties_Georgia.geojson`
3. Quarantine any row outside Georgia polygons or any row without an assignable county
4. If an existing county label disagrees with `computed_county`, log it
5. Generate:
   - `geo_validation_report.csv`
   - mismatch-rate summary
6. Exclude quarantined rows from:
   - the map
   - DuckDB analytics
   - FAISS indexing
   - geo retrieval

Why this ordering matters:

- Quarantine alone is not enough.
- If the upstream join is wrong, quarantine can hide the root cause by producing a tiny “clean” subset while the real data problem remains unresolved.
- The coordinate-join audit must therefore run first and must prove:
  - the join key is correct
  - the match rate is acceptable
  - duplicate keys are understood
  - sample company/address/coordinate pairings make sense on inspection

### Current Gate Status: FAIL

Observed from the current built artifacts:

- `207` companies total in DuckDB
- `202` rows use the external coordinate workbook
- `4` rows still use `county_centroid`
- `1` row is still `missing`

Observed from polygon validation against `Counties_Georgia.geojson`:

- `9` company points are outside all Georgia county polygons
- `167` of `183` rows with both county labels and coordinates do **not** fall inside their labeled county polygon

This is not a small edge case. It is a fundamental geo-integrity problem.
This pattern is large enough that a bad join or systematically misaligned coordinate workbook is more likely than a handful of isolated bad points.

Refined interpretation from the stricter workbook audit:

- duplicate coordinates do exist, but they do **not** explain the problem away
- after collapsing repeated rows to unique facilities, the county mismatch rate still remains about `90%`
- even after removing the entire duplicate-coordinate subset, the remaining non-duplicate facilities still mismatch at about `94%`

So the core issue is not just multi-role duplication. The stronger evidence points to a broken join, contaminated workbook, mixed facility/HQ records, or a combination of those upstream problems.

### Required Fail-Fast Thresholds

The ingestion pipeline should not be allowed to “succeed” while silently dropping a large fraction of the dataset.

Recommended thresholds:

- if `outside_ga_rate > 1%` -> fail ingestion and require coordinate/join fix
- if `county_mismatch_rate > 5%` to `10%` -> fail ingestion and require join/coordinate fix

Given the current observed rates, the present build should fail this gate rather than proceed.

Research-grade interpretation:

- these thresholds are not optional hygiene checks
- they are stop conditions that prevent the system from publishing geometry-backed claims from a corrupted geo layer
- if they are exceeded, ingestion should fail loudly and require a data fix rather than silently proceeding with quarantine

### Concrete Coordinate Evidence

Examples from `data/GNEM - Auto Landscape Lat Long Updated File (1).xlsx`:

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

These examples are enough to conclude that the current coordinate feed cannot be trusted without validation and quarantine.

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

### Entrypoints

- Ingestion: `backend/ingestion.py`
- API: `backend/main.py`
- Frontend: `frontend/app.py`
- Query planner: `backend/query_planner.py`
- Spatial logic: `backend/spatial_engine.py`
- RAG orchestrator: `backend/rag_pipeline.py`
- Vector retrieval / embeddings: `backend/vector_engine.py`
- Logging: no dedicated module currently exists

### Architecture Summary

- `frontend/app.py`
  - Streamlit app
  - uses `pydeck`
  - renders point heatmap + point scatter
  - no county GeoJSON layer

- `backend/main.py`
  - FastAPI app
  - loads a single `HybridGeospatialRAGPipeline` on startup
  - exposes `/health` and `/chat`

- `backend/ingestion.py`
  - loads Excel company data
  - loads external coordinate workbook
  - parses city/county from text
  - falls back to county centroid when coordinates are missing
  - writes DuckDB + FAISS + vector metadata

- `backend/spatial_engine.py`
  - builds city “centroids” by averaging stored points
  - builds county centroids from the GeoJSON
  - answers point-radius queries only

- `backend/query_planner.py`
  - heuristic parser for coordinates, radius, city, OEM, category, capability
  - no explicit county intent or gap-query intent

- `backend/rag_pipeline.py`
  - merges SQL, vector, and geo retrieval
  - builds generic retrieval chunks
  - asks Ollama for final answer
  - uses generic `[C1]`-style citations

- `backend/vector_engine.py`
  - loads FAISS
  - may use sentence-transformers
  - silently falls back to hash mode if load fails

### Mapping Library and Recommendation

Current library:

- `pydeck`

Current feasibility:

- Polygon overlay: feasible
- Hover tooltips: feasible
- Reliable polygon click selection back into Python: not implemented and not dependable in this app as written

Recommendation for `<300` points:

- Keep `pydeck` if the default interaction is a **county dropdown**, which is your required reliable path.
- Only pursue polygon-click selection if a dependable event bridge is introduced.
- If reliable click selection becomes a hard requirement, `streamlit-folium` is the safer alternative for this app size.

## B) Current Behavior by Subsystem

### Frontend Map

`frontend/app.py::render_map`

- renders:
  - `HeatmapLayer`
  - `ScatterplotLayer`
- does not render:
  - `GeoJsonLayer`
  - `PolygonLayer`
  - county dropdown
  - county summary panel
  - choropleth

### County Assignment

`backend/ingestion.py::attach_coordinates`

- current county is taken from:
  - parsed `Location` text
  - coordinate workbook columns
- it is **not** computed via point-in-polygon

### County Geometry

`backend/ingestion.py::load_county_centroids`

- reduces each county polygon to an average-of-vertices point

`backend/spatial_engine.py::companies_near_city`

- resolves county-like queries to a centroid point and then applies point-radius filtering

That violates your non-negotiable definition of county distance.

### Point Radius Queries

`backend/spatial_engine.py::companies_within_radius`

- uses `geopy.distance`
- this is directionally correct for point-radius queries
- but the stored company coordinates are currently unreliable

### Query Routing

`backend/query_planner.py::plan`

- route types currently returned:
  - `SQL_QUERY`
  - `GEO_QUERY`
  - `VECTOR_QUERY`
  - `HYBRID_QUERY`

Missing route transparency:

- no explicit `lookup`
- no explicit `analytic_local`
- no explicit `web_needed`

### Evidence and Citations

`backend/rag_pipeline.py`

- builds generic chunk ids `C1`, `C2`, ...
- prompts the LLM to cite chunk ids like `[C3]`
- does not emit:
  - `DOC:...`
  - `GEO:...`
  - `ANALYTIC:...`
- does not perform post-generation citation validation

## C) Requirements Checklist

| # | Requirement | Status | Evidence | Assessment |
| --- | --- | --- | --- | --- |
| 1 | County polygon overlay visible on map | MISSING | `frontend/app.py::render_map` | No county GeoJSON layer is rendered. |
| 2 | Company point layer visible with tooltips (`name`, `role/category`, `county`, `lat/lon`) | PARTIAL | `frontend/app.py::render_map` | Points and tooltips exist, but tooltips omit explicit `lat/lon` and do not clearly expose role/category + county in the required format. |
| 3 | Choropleth shading by county counts | MISSING | no implementation found | No county aggregation + polygon fill path exists. |
| 4 | County selection interaction (`dropdown` required) | MISSING | `frontend/app.py` sidebar + map | No county dropdown and no county-selection state. |
| 5 | Correct county assignment (point-in-polygon from GeoJSON) | MISSING | `backend/ingestion.py::attach_coordinates` | County is text/workbook-derived, not geometry-derived. |
| 6 | Correct “within X miles of county” using point-to-polygon minimum distance in projected CRS | MISSING | `backend/ingestion.py::load_county_centroids`, `backend/spatial_engine.py::companies_near_city` | County queries use centroid points, not polygon boundary distance and not projected CRS distance. |
| 7 | Correct “within X miles of point” (haversine/geodesic) | PARTIAL | `backend/spatial_engine.py::companies_within_radius` | Uses `geopy.distance`, but point correctness is undermined by unvalidated coordinates. |
| 8 | Derived analytic summaries exist (county counts, role distribution, zero-count gaps) | MISSING | no analytic engine/table found | No deterministic county analytic tables exist. |
| 9 | Geo-aware RAG routing exists and is transparent (`lookup`, `analytic_local`, `web_needed`) | MISSING | `backend/query_planner.py::plan`, `backend/rag_pipeline.py::answer_question` | Current routing is heuristic and opaque; no explicit route taxonomy or auditability. |
| 10 | Strict evidence IDs `DOC:`, `GEO:`, `ANALYTIC:` | MISSING | `backend/rag_pipeline.py::_build_retrieved_chunks`, `_generate_answer_with_llm` | Generic `[C1]` ids are used instead of stable evidence namespaces. |
| 11 | Reproducibility: pinned deps + deterministic embedding loading, no silent fallback | MISSING | `requirements.txt`, `backend/ingestion.py::create_embeddings`, `backend/vector_engine.py::_init_embedder` | Dependencies are unpinned and embedding fallback is silent. |
| 12 | Minimal pytest suite exists | MISSING | no `tests/` directory | No automated tests. |
| 13 | JSONL logging exists | MISSING | `backend/main.py` | No structured query/result/evidence logging exists. |

## D) Why the Current Implementation Fails the Intended Requirements

### 1. It is not yet a true GeoJSON county map

The codebase loads a county GeoJSON file during ingestion, but the frontend never displays those polygons. The user sees point blobs, not counties.

### 2. County math is currently geometry-wrong

The code uses county centroids, and those centroids are not even proper polygon centroids. They are averages of vertices. That is unacceptable for:

- county containment
- county-distance queries
- county gap analytics

### 3. Coordinate integrity is not enforced

This is the biggest professor-risk in the repository. If points are not validated before indexing and display, the map, retrieval, and RAG outputs are all vulnerable to invalid geographic claims.

### 4. County and gap queries are being answered with the wrong machinery

Questions like:

- `show companies in Fulton County`
- `within 50 miles of Fulton County`
- `which counties have 0 battery companies`

should be routed to:

- deterministic geometry lookup
- deterministic local analytics

They should not be left to heuristic semantic retrieval plus LLM narration.

### 5. The retrieval and citation layer is not geo-transparent

The current evidence layer tells the model “cite `[C3]`,” but professor-defensible geo/RAG systems need citations that disclose whether the supporting fact came from:

- a source document
- a polygon computation
- a deterministic analytic table

## E) Prioritized Change Plan

## P0: Summit-Blocking Correctness

### P0.0 Add the geo correctness gate first

This must begin with a coordinate-join audit, not immediate quarantine.

- What to change:
  - Audit the coordinate join, then validate every point against the county GeoJSON before it can enter the system.
- Where:
  - `backend/ingestion.py`
- Why:
  - This is your non-negotiable correctness gate.
- How:
  - First, write a join audit:
    - join key used
    - exact-match rate
    - duplicate key count
    - examples of bad company/address/coordinate pairings
  - Load county polygons
  - Compute `computed_county` by point-in-polygon
  - If point is outside Georgia or not assignable, quarantine it
  - If `county` mismatches `computed_county`, log it
  - Write:
    - `geo_validation_report.csv`
    - mismatch-rate summary
  - Enforce fail-fast thresholds:
    - `outside_ga_rate > 1%` -> fail ingestion
    - `county_mismatch_rate > 5%` to `10%` -> fail ingestion
  - Exclude quarantined rows from DuckDB, FAISS, and UI payloads
  - Do not mark the pipeline “healthy” if the join audit fails, even if a small quarantine-safe subset exists

### P0.1 Add county GeoJSON overlay and county dropdown

- What to change:
  - Add county polygons, county tooltip counts, and county dropdown.
- Where:
  - `frontend/app.py`
- Why:
  - This is the required map interaction surface and the safest selection mode.
- How:
  - Load `Counties_Georgia.geojson` into the frontend
  - Add a county dropdown in the sidebar
  - Highlight the selected county polygon
  - Add county tooltips:
    - county name
    - filtered company count
    - total company count
    - role/category stats
  - Keep polygon click optional

### P0.2 Replace centroid county logic with polygon-aware spatial operations

- What to change:
  - Replace county centroid logic entirely for county containment and county distance.
- Where:
  - `backend/spatial_engine.py`
  - `backend/query_planner.py`
  - `backend/rag_pipeline.py`
- Why:
  - The current approach is not geometrically valid.
- How:
  - Load county polygons once using `shapely`
  - Project points and polygons to a projected CRS in meters before county-distance calculations
  - Standardize on `EPSG:5070` for statewide distance math
  - Use the same CRS in code, tests, logs, and evidence payloads
  - Implement:
    - `companies_in_county(county_name)`
    - `companies_within_miles_of_county(county_name, miles)`
    - `companies_within_miles_of_point(lat, lon, miles)`
  - County distance definition:
    - minimum point-to-polygon distance in projected meters
    - convert meters to miles
    - distance is `0` if the point lies inside the polygon

### P0.3 Extend planner to parse county and gap intents explicitly

- What to change:
  - Add explicit parsing for county lookup, county-distance, and gap analytics.
- Where:
  - `backend/query_planner.py`
- Why:
  - Current planner misroutes county and gap questions.
- How:
  - Add support for:
    - `in <county>`
    - `within <N> miles of <county>`
    - `counties with 0 <role/category>`
  - Produce explicit route tags:
    - `lookup`
    - `analytic_local`
    - `llm_synthesis`
    - `web_needed`
  - Add structured hints:
    - `target_county`
    - `radius_miles`
    - `geo_anchor_type`
    - `analytic_metric`

### P0.4 Route county/gap questions to deterministic local paths

- What to change:
  - Do not answer county analytics via semantic search alone.
- Where:
  - `backend/rag_pipeline.py`
  - recommended new module: `backend/analytics_engine.py`
- Why:
  - County queries are local analytic/geometry tasks.
- How:
  - Route:
    - county membership -> geometry lookup
    - county distance -> geometry lookup
    - zero-count gap -> deterministic analytic table
  - Reserve `llm_synthesis` only for narration over already-computed deterministic evidence
  - Define `llm_synthesis` operationally as:
    - LLM after deterministic `GEO:` and/or `ANALYTIC:` evidence has already been produced
    - never LLM-only county reasoning
  - Never let the LLM invent the county set, gap set, or county-distance result

### P0.5 Replace generic citations with stable evidence IDs

- What to change:
  - Introduce stable evidence namespaces.
- Where:
  - `backend/rag_pipeline.py`
- Why:
  - Generic `[C1]` ids are not stable or audit-friendly.
- How:
  - Emit:
    - `DOC:<chunk_id>`
    - `GEO:within_miles_of_county|county=<county>|company=<slug>|dist_mi=<value>|crs=EPSG:5070`
    - `ANALYTIC:<table>|<county>|<metric>`
  - Prefer evidence IDs that expose the computed value, the target geography, and the CRS so the claim is directly auditable
  - Carry these ids into:
    - retrieved chunk records
    - answer prompt
    - UI evidence panel

### P0.6 Add post-generation citation validation

- What to change:
  - Validate every non-abstaining bullet in the final answer.
- Where:
  - `backend/rag_pipeline.py`
- Why:
  - Geo/RAG answers must not ship uncited claims.
- How:
  - After generation:
    - parse bullets
    - require citation tokens on each non-abstaining bullet
    - allowed tokens:
      - `[DOC:...]`
      - `[GEO:...]`
      - `[ANALYTIC:...]`
  - If validation fails:
    - force abstention
    - or replace with a deterministic fallback summary

## P1: Reproducibility + Rigor

### P1.1 Materialize county analytics tables in DuckDB

- What to change:
  - Add deterministic county-level summary tables.
- Where:
  - `backend/ingestion.py`
  - new `backend/analytics_engine.py`
  - DuckDB output
- Required tables:
  - company count per county
  - role distribution per county
  - category distribution per county
  - zero-count gap-support tables

### P1.2 Pin dependencies and add geometry/test packages

- What to change:
  - Replace loose requirements with pinned versions.
- Where:
  - `requirements.txt`
  - optional lockfile
- Add:
  - `shapely`
  - `pyproj`
  - `geopandas` if preferred
  - `pytest`
  - `streamlit-folium` only if click interaction becomes necessary

### P1.3 Remove silent embedding fallback

- What to change:
  - Make embedding backend explicit and consistent between ingestion and query time.
- Where:
  - `backend/ingestion.py::create_embeddings`
  - `backend/vector_engine.py::_init_embedder`
- Why:
  - Silent embedding fallback undermines reproducibility and can create invalid index/query mixtures.
- How:
  - Fail fast if the configured embedding backend is unavailable
  - Or explicitly choose a deterministic fallback mode and rebuild the entire index consistently
  - Never silently mix backends

### P1.4 Remove live Nominatim fallback

- What to change:
  - Delete runtime geocoder dependence.
- Where:
  - `backend/spatial_engine.py::_resolve_city_coordinates`
- Why:
  - It is not reproducible and not necessary for the stated county-first workflow.

### P1.5 Add minimal pytest suite

- What to change:
  - Add a `tests/` directory with at least five cases.
- Required tests:
  1. parse radius + county intent correctly
  2. point-in-polygon assigns expected county for a known test point
  3. polygon-distance query works for a boundary-near point
  4. gap analytics returns deterministic output for a known role/category
  5. citation enforcement rejects an uncited bullet

### P1.6 Add JSONL logging

- What to change:
  - Add structured per-request logs.
- Where:
  - `backend/main.py`
  - recommended new helper: `backend/logging_utils.py`
- Required fields:
  - timestamp
  - query
  - route/plan
  - selected county / radius / anchor
  - evidence ids
  - retrieval summary
  - model
  - embedding backend
  - answer
  - errors

### P1.7 Add a deterministic geo “truth slice” for evaluation

- What to change:
  - Create a small evaluation slice where correctness can be claimed without web lookup.
- Why:
  - This is the most defensible research-grade accuracy story for the geo subsystem.
- Include:
  - county assignment via point-in-polygon
  - within-X-miles-of-county via polygon distance in `EPSG:5070`
  - zero-count county gap queries
- Use this slice for:
  - before/after coordinate-join audit comparisons
  - centroid-vs-polygon ablations
  - citation-validator ablations

## P2: Polish

### P2.1 Add choropleth shading and county summary side panel

- Where:
  - `frontend/app.py`

### P2.2 Surface geo method choice in the UI

- Where:
  - `frontend/app.py`
- Show:
  - polygon containment
  - polygon-distance in projected CRS
  - geodesic point-radius

### P2.3 Improve backend startup diagnostics

- Where:
  - `frontend/app.py`
- Why:
  - Current auto-start swallows backend stdout/stderr

## F) File / Function Edit Targets

### Core edit list

- `backend/ingestion.py`
  - `attach_coordinates`
  - `run_ingestion`
  - add geo validation + quarantine + report generation

- `backend/spatial_engine.py`
  - replace centroid logic
  - add polygon containment and projected-CRS county distance
  - remove live geocoder fallback

- `backend/query_planner.py`
  - add county intent parser
  - add gap-query parser
  - add route transparency fields

- `backend/rag_pipeline.py`
  - add deterministic route handling
  - add geo/doc/analytic evidence ids
  - add citation validator

- `frontend/app.py`
  - add county dropdown
  - add county polygon layer
  - add county summary panel
  - add tooltip lat/lon

- `requirements.txt`
  - pin dependencies
  - add geometry/test stack

- `backend/main.py`
  - add JSONL logging hooks

## G) Recommended Summit Demo Workflow

Once P0 is complete, demo in this order:

1. Select a county from a dropdown
2. Show the highlighted GeoJSON county polygon
3. Show company points inside that county with tooltips
4. Ask:
   - `show companies in Fulton County`
5. Ask:
   - `which battery suppliers are within 50 miles of Fulton County`
6. Ask:
   - `which counties have 0 battery companies`
7. Open evidence panel showing:
   - `DOC:...`
   - `GEO:...`
   - `ANALYTIC:...`
8. Show JSONL run log and geo validation report

## Final Assessment

If judged as a prototype:

- promising

If judged as a research-grade, geometry-correct, GeoJSON county RAG system:

- **not yet acceptable**

The blocking reasons are:

1. no county polygon map in the UI
2. no point-in-polygon county assignment
3. no projected-CRS point-to-polygon county distance
4. no geo validation gate before indexing
5. no deterministic county analytics
6. no stable evidence ID scheme
7. no reproducibility discipline
