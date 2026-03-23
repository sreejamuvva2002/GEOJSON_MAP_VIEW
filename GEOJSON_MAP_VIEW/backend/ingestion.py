from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import duckdb
import faiss
import numpy as np
import pandas as pd

DEFAULT_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
EMBED_DIMENSION = 384

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_EXCEL_PATH = DATA_DIR / "gnem_companies.xlsx"
DEFAULT_GEOJSON_PATH = DATA_DIR / "Counties_Georgia.geojson"
DEFAULT_COORDINATE_EXCEL_PATH = DATA_DIR / "company_coordinates.xlsx"
DEFAULT_DB_PATH = DATA_DIR / "gnem.duckdb"
DEFAULT_FAISS_PATH = DATA_DIR / "gnem_faiss.index"
DEFAULT_METADATA_PATH = DATA_DIR / "vector_metadata.json"

COMPANY_COLUMN_CANDIDATES = ["company", "company_name", "supplier", "supplier_name", "name"]
LOCATION_COLUMN_CANDIDATES = ["location", "facility_location", "address", "city_county", "site"]
CITY_COLUMN_CANDIDATES = ["city", "municipality", "town"]
COUNTY_COLUMN_CANDIDATES = ["county", "county_name"]
LATITUDE_COLUMN_CANDIDATES = ["latitude", "lat", "facility_latitude", "y"]
LONGITUDE_COLUMN_CANDIDATES = ["longitude", "lon", "lng", "long", "facility_longitude", "x"]


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    def _clean(name: str) -> str:
        cleaned = re.sub(r"[^0-9a-zA-Z]+", "_", str(name).strip().lower())
        return cleaned.strip("_")

    df = df.copy()
    df.columns = [_clean(col) for col in df.columns]
    return df


def normalize_cell(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def normalize_match_key(value: object) -> str:
    text = normalize_cell(value).lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def extract_city_county(location: object) -> Tuple[Optional[str], Optional[str]]:
    if pd.isna(location):
        return None, None

    text = str(location).strip()
    if not text:
        return None, None

    parts = [p.strip() for p in text.split(",") if p.strip()]
    city = parts[0].title() if parts else None

    county_match = re.search(r"([A-Za-z][A-Za-z\s\-']+?)\s+County", text, flags=re.IGNORECASE)
    county = county_match.group(1).strip().title() if county_match else None

    if not county and len(parts) > 1:
        maybe_county = parts[1].replace("County", "").strip()
        county = maybe_county.title() if maybe_county else None

    return city, county


def _iter_coordinates(geometry: dict) -> Iterable[Tuple[float, float]]:
    geom_type = geometry.get("type")
    coords = geometry.get("coordinates", [])

    if geom_type == "Polygon":
        for ring in coords:
            for lon, lat in ring:
                yield float(lat), float(lon)
    elif geom_type == "MultiPolygon":
        for polygon in coords:
            for ring in polygon:
                for lon, lat in ring:
                    yield float(lat), float(lon)


def _mean_lat_lon(points: List[Tuple[float, float]]) -> Optional[Tuple[float, float]]:
    if not points:
        return None
    arr = np.array(points, dtype=np.float32)
    lat = float(arr[:, 0].mean())
    lon = float(arr[:, 1].mean())
    return lat, lon


def load_county_centroids(geojson_path: Path) -> Dict[str, Tuple[float, float]]:
    with geojson_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    centroids: Dict[str, Tuple[float, float]] = {}
    for feature in payload.get("features", []):
        props = feature.get("properties", {})
        geometry = feature.get("geometry", {})
        points = list(_iter_coordinates(geometry))
        centroid = _mean_lat_lon(points)
        if not centroid:
            continue

        county_name = (
            props.get("NAME10")
            or props.get("NAME")
            or props.get("NAMELSAD10", "").replace("County", "").strip()
        )
        if county_name:
            key = str(county_name).strip().lower()
            centroids[key] = centroid

    return centroids


def _safe_float(value: object) -> Optional[float]:
    if pd.isna(value):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _find_first_present(columns: List[str], candidates: List[str]) -> Optional[str]:
    column_lookup = {col.lower(): col for col in columns}
    for candidate in candidates:
        if candidate.lower() in column_lookup:
            return column_lookup[candidate.lower()]
    return None


def _detect_coordinate_columns(df: pd.DataFrame) -> Optional[Dict[str, str]]:
    cols = list(df.columns)
    company_col = _find_first_present(cols, COMPANY_COLUMN_CANDIDATES)
    lat_col = _find_first_present(cols, LATITUDE_COLUMN_CANDIDATES)
    lon_col = _find_first_present(cols, LONGITUDE_COLUMN_CANDIDATES)
    if not company_col or not lat_col or not lon_col:
        return None

    detected = {
        "company": company_col,
        "latitude": lat_col,
        "longitude": lon_col,
    }
    optional_pairs = {
        "location": LOCATION_COLUMN_CANDIDATES,
        "city": CITY_COLUMN_CANDIDATES,
        "county": COUNTY_COLUMN_CANDIDATES,
    }
    for key, candidates in optional_pairs.items():
        optional_col = _find_first_present(cols, candidates)
        if optional_col:
            detected[key] = optional_col
    return detected


def discover_coordinate_workbook(explicit_path: Path = DEFAULT_COORDINATE_EXCEL_PATH) -> Optional[Path]:
    if explicit_path.exists():
        return explicit_path

    preferred_names = [
        "GNEM - Auto Landscape Lat Long Updated File (1).xlsx",
        "company_coordinates.xlsx",
    ]
    for filename in preferred_names:
        candidate = DATA_DIR / filename
        if candidate.exists():
            return candidate

    excluded_names = {"gnem_companies.xlsx", "gnem updated excel.xlsx"}
    search_dirs = [DATA_DIR, PROJECT_ROOT.parent]
    for search_dir in search_dirs:
        if not search_dir.exists():
            continue
        for pattern in ("*.xlsx", "*.xls"):
            for workbook in sorted(search_dir.glob(pattern)):
                if workbook.name.lower() in excluded_names:
                    continue
                try:
                    xls = pd.ExcelFile(workbook)
                    for sheet_name in xls.sheet_names:
                        preview = clean_columns(pd.read_excel(workbook, sheet_name=sheet_name, nrows=5))
                        if _detect_coordinate_columns(preview):
                            return workbook
                except Exception:
                    continue
    return None


def load_coordinate_enrichment(workbook_path: Optional[Path]) -> Tuple[pd.DataFrame, Optional[str]]:
    if workbook_path is None or not workbook_path.exists():
        return pd.DataFrame(), None

    xls = pd.ExcelFile(workbook_path)
    for sheet_name in xls.sheet_names:
        preview = clean_columns(pd.read_excel(workbook_path, sheet_name=sheet_name, nrows=5))
        detected = _detect_coordinate_columns(preview)
        if not detected:
            continue

        full_df = clean_columns(pd.read_excel(workbook_path, sheet_name=sheet_name))
        rename_map = {
            detected["company"]: "coord_company",
            detected["latitude"]: "coord_latitude",
            detected["longitude"]: "coord_longitude",
        }
        for optional_key in ("location", "city", "county"):
            optional_col = detected.get(optional_key)
            if optional_col:
                rename_map[optional_col] = f"coord_{optional_key}"

        coords = full_df.rename(columns=rename_map)
        keep_cols = [col for col in coords.columns if col.startswith("coord_")]
        coords = coords[keep_cols].copy()
        coords["coord_company_key"] = coords["coord_company"].apply(normalize_match_key)
        if "coord_location" in coords.columns:
            coords["coord_location_key"] = coords["coord_location"].apply(normalize_match_key)
        else:
            coords["coord_location_key"] = ""

        coords["coord_latitude"] = pd.to_numeric(coords["coord_latitude"], errors="coerce")
        coords["coord_longitude"] = pd.to_numeric(coords["coord_longitude"], errors="coerce")
        coords = coords.dropna(subset=["coord_latitude", "coord_longitude"]).copy()
        coords = coords[
            (coords["coord_latitude"].between(-90, 90))
            & (coords["coord_longitude"].between(-180, 180))
        ].copy()
        if coords.empty:
            continue

        coords["coordinate_source_file"] = workbook_path.name
        coords["coordinate_source_sheet"] = sheet_name
        return coords.reset_index(drop=True), f"{workbook_path.name}::{sheet_name}"

    return pd.DataFrame(), None


def attach_coordinates(
    df: pd.DataFrame,
    county_centroids: Dict[str, Tuple[float, float]],
    coordinate_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    out = df.copy()
    cities: List[Optional[str]] = []
    counties: List[Optional[str]] = []
    lats: List[Optional[float]] = []
    lons: List[Optional[float]] = []
    sources: List[str] = []

    if "latitude" in out.columns:
        out["latitude"] = pd.to_numeric(out["latitude"], errors="coerce")
    else:
        out["latitude"] = np.nan
    if "longitude" in out.columns:
        out["longitude"] = pd.to_numeric(out["longitude"], errors="coerce")
    else:
        out["longitude"] = np.nan

    out["company_key"] = out.get("company", "").apply(normalize_match_key)
    out["location_key"] = out.get("location", "").apply(normalize_match_key)

    exact_coords = pd.DataFrame()
    company_coords = pd.DataFrame()
    if coordinate_df is not None and not coordinate_df.empty:
        exact_coords = coordinate_df[coordinate_df["coord_location_key"] != ""].copy()
        exact_coords = exact_coords.sort_values(["coord_company_key", "coord_location_key"]).drop_duplicates(
            subset=["coord_company_key", "coord_location_key"],
            keep="first",
        )
        company_coords = coordinate_df.sort_values(["coord_company_key"]).drop_duplicates(
            subset=["coord_company_key"],
            keep="first",
        )
        out = out.merge(
            exact_coords[
                [
                    "coord_company_key",
                    "coord_location_key",
                    "coord_latitude",
                    "coord_longitude",
                    "coordinate_source_file",
                    "coordinate_source_sheet",
                ]
            ].rename(
                columns={
                    "coord_company_key": "exact_company_key",
                    "coord_location_key": "exact_location_key",
                    "coord_latitude": "exact_latitude",
                    "coord_longitude": "exact_longitude",
                    "coordinate_source_file": "exact_coordinate_source_file",
                    "coordinate_source_sheet": "exact_coordinate_source_sheet",
                }
            ),
            left_on=["company_key", "location_key"],
            right_on=["exact_company_key", "exact_location_key"],
            how="left",
        )
        out = out.merge(
            company_coords[
                [
                    "coord_company_key",
                    "coord_latitude",
                    "coord_longitude",
                    "coordinate_source_file",
                    "coordinate_source_sheet",
                    *([c for c in ["coord_city", "coord_county"] if c in company_coords.columns]),
                ]
            ].rename(
                columns={
                    "coord_company_key": "company_coord_key",
                    "coord_latitude": "company_latitude",
                    "coord_longitude": "company_longitude",
                    "coordinate_source_file": "company_coordinate_source_file",
                    "coordinate_source_sheet": "company_coordinate_source_sheet",
                    "coord_city": "company_coord_city",
                    "coord_county": "company_coord_county",
                }
            ),
            left_on="company_key",
            right_on="company_coord_key",
            how="left",
        )

    for _, row in out.iterrows():
        city, county = extract_city_county(row.get("location"))
        explicit_city = normalize_cell(row.get("company_coord_city"))
        explicit_county = normalize_cell(row.get("company_coord_county"))
        if not city and explicit_city:
            city = explicit_city.title()
        if not county and explicit_county:
            county = explicit_county.title()
        cities.append(city)
        counties.append(county)

        lat = _safe_float(row.get("latitude"))
        lon = _safe_float(row.get("longitude"))
        source = "source_excel" if lat is not None and lon is not None else "missing"

        exact_lat = _safe_float(row.get("exact_latitude"))
        exact_lon = _safe_float(row.get("exact_longitude"))
        company_lat = _safe_float(row.get("company_latitude"))
        company_lon = _safe_float(row.get("company_longitude"))
        if lat is None or lon is None:
            if exact_lat is not None and exact_lon is not None:
                lat, lon = exact_lat, exact_lon
                source = f"coordinates_excel:{normalize_cell(row.get('exact_coordinate_source_file')) or 'external'}"
            elif company_lat is not None and company_lon is not None:
                lat, lon = company_lat, company_lon
                source = f"coordinates_excel:{normalize_cell(row.get('company_coordinate_source_file')) or 'external'}"

        if (lat is None or lon is None) and county:
            county_key = county.strip().lower()
            if county_key in county_centroids:
                lat, lon = county_centroids[county_key]
                source = "county_centroid"

        if lat is None or lon is None:
            source = "missing"

        lats.append(lat)
        lons.append(lon)
        sources.append(source)

    out["city"] = cities
    out["county"] = counties
    out["latitude"] = lats
    out["longitude"] = lons
    out["coordinate_source"] = sources
    drop_cols = [
        "company_key",
        "location_key",
        "exact_company_key",
        "exact_location_key",
        "exact_latitude",
        "exact_longitude",
        "exact_coordinate_source_file",
        "exact_coordinate_source_sheet",
        "company_coord_key",
        "company_latitude",
        "company_longitude",
        "company_coordinate_source_file",
        "company_coordinate_source_sheet",
        "company_coord_city",
        "company_coord_county",
    ]
    existing_drop_cols = [col for col in drop_cols if col in out.columns]
    return out.drop(columns=existing_drop_cols)


def _company_slug(company: str, fallback_idx: int) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", company.lower()).strip("-")
    return slug or f"company-{fallback_idx}"


def build_chunk_records(df: pd.DataFrame) -> List[dict]:
    records: List[dict] = []
    for row_idx, (_, row) in enumerate(df.iterrows()):
        company = normalize_cell(row.get("company")) or f"Unknown Company {row_idx + 1}"
        company_slug = _company_slug(company, row_idx + 1)

        category = normalize_cell(row.get("category"))
        industry_group = normalize_cell(row.get("industry_group"))
        location = normalize_cell(row.get("location"))
        facility_type = normalize_cell(row.get("primary_facility_type"))
        ev_role = normalize_cell(row.get("ev_supply_chain_role"))
        primary_oems = normalize_cell(row.get("primary_oems"))
        affiliation_type = normalize_cell(row.get("supplier_or_affiliation_type"))
        product_service = normalize_cell(row.get("product_service"))
        ev_relevant = normalize_cell(row.get("ev_battery_relevant"))
        classification = normalize_cell(row.get("classification_method"))
        city = normalize_cell(row.get("city"))
        county = normalize_cell(row.get("county"))
        employment = _safe_float(row.get("employment"))
        latitude = _safe_float(row.get("latitude"))
        longitude = _safe_float(row.get("longitude"))
        coordinate_source = normalize_cell(row.get("coordinate_source"))

        base = {
            "company": company,
            "category": category,
            "industry_group": industry_group,
            "location": location,
            "city": city,
            "county": county,
            "ev_supply_chain_role": ev_role,
            "primary_oems": primary_oems,
            "supplier_or_affiliation_type": affiliation_type,
            "employment": employment,
            "product_service": product_service,
            "ev_battery_relevant": ev_relevant,
            "classification_method": classification,
            "primary_facility_type": facility_type,
            "latitude": latitude,
            "longitude": longitude,
            "coordinate_source": coordinate_source,
            "row_index": int(row_idx),
            "source_dataset": "GNEM updated excel.xlsx",
        }

        chunk_templates = {
            "company_profile": (
                "Company Profile\n"
                f"Company: {company}\n"
                f"Category: {category}\n"
                f"Industry Group: {industry_group}\n"
                f"Primary Facility Type: {facility_type}\n"
                f"EV Supply Chain Role: {ev_role}\n"
                f"Classification Method: {classification}"
            ),
            "supply_chain": (
                "Supply Chain Relationships\n"
                f"Company: {company}\n"
                f"Primary OEMs: {primary_oems}\n"
                f"Supplier / Affiliation Type: {affiliation_type}\n"
                f"EV / Battery Relevant: {ev_relevant}\n"
                f"EV Supply Chain Role: {ev_role}"
            ),
            "products_capabilities": (
                "Products and Capabilities\n"
                f"Company: {company}\n"
                f"Product / Service: {product_service}\n"
                f"Industry Group: {industry_group}\n"
                f"Category: {category}"
            ),
            "geo_operations": (
                "Geographic Operations\n"
                f"Company: {company}\n"
                f"Location: {location}\n"
                f"City: {city}\n"
                f"County: {county}\n"
                f"Latitude: {latitude if latitude is not None else 'unknown'}\n"
                f"Longitude: {longitude if longitude is not None else 'unknown'}\n"
                f"Coordinate Source: {coordinate_source or 'unknown'}\n"
                f"Employment: {int(employment) if employment is not None else 'unknown'}"
            ),
        }

        for chunk_type, chunk_text in chunk_templates.items():
            records.append(
                {
                    **base,
                    "chunk_id": f"{company_slug}:{row_idx}:{chunk_type}",
                    "chunk_type": chunk_type,
                    "chunk_text": chunk_text.strip(),
                }
            )
    return records


def _hash_embed_one(text: str, dim: int = EMBED_DIMENSION) -> np.ndarray:
    vec = np.zeros(dim, dtype=np.float32)
    tokens = re.findall(r"[a-z0-9]+", text.lower())

    for token in tokens:
        token_hash = int(hashlib.md5(token.encode("utf-8")).hexdigest(), 16)
        idx = token_hash % dim
        sign = 1.0 if ((token_hash >> 1) & 1) else -1.0
        vec[idx] += sign

    norm = np.linalg.norm(vec)
    if norm > 0:
        vec = vec / norm
    return vec.astype(np.float32)


def create_embeddings(
    docs: List[str],
    model_name: str = DEFAULT_MODEL_NAME,
) -> Tuple[np.ndarray, str, str]:
    import os

    backend_pref = os.getenv("EMBEDDING_BACKEND", "hash").strip().lower()
    if backend_pref not in {"sentence-transformers", "sbert", "sentence"}:
        fallback = np.vstack([_hash_embed_one(doc) for doc in docs]).astype(np.float32)
        return fallback, "hash-fallback", "hashed-token-384"

    try:
        from sentence_transformers import SentenceTransformer

        local_only = os.getenv("EMBEDDING_LOCAL_ONLY", "true").strip().lower() == "true"
        model = SentenceTransformer(model_name, local_files_only=local_only)
        embeddings = model.encode(
            docs,
            normalize_embeddings=True,
            convert_to_numpy=True,
            show_progress_bar=True,
        ).astype(np.float32)
        return embeddings, "sentence-transformers", model_name
    except Exception as exc:
        print(f"[ingestion] Embedding model unavailable, using hash fallback: {exc}")
        fallback = np.vstack([_hash_embed_one(doc) for doc in docs]).astype(np.float32)
        return fallback, "hash-fallback", "hashed-token-384"


def write_duckdb(df: pd.DataFrame, db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as con:
        con.register("df_companies", df)
        con.execute("CREATE OR REPLACE TABLE companies AS SELECT * FROM df_companies")


def write_company_chunks_duckdb(chunk_records: List[dict], db_path: Path) -> None:
    if not chunk_records:
        return
    with duckdb.connect(str(db_path)) as con:
        chunk_df = pd.DataFrame(chunk_records)
        con.register("df_company_chunks", chunk_df)
        con.execute("CREATE OR REPLACE TABLE company_chunks AS SELECT * FROM df_company_chunks")


def write_faiss(embeddings: np.ndarray, faiss_path: Path) -> None:
    faiss_path.parent.mkdir(parents=True, exist_ok=True)
    vectors = embeddings.astype(np.float32)
    faiss.normalize_L2(vectors)
    index = faiss.IndexFlatIP(vectors.shape[1])
    index.add(vectors)
    faiss.write_index(index, str(faiss_path))


def write_vector_metadata(
    chunk_records: List[dict],
    vector_dim: int,
    metadata_path: Path,
    embedding_backend: str,
    embedding_model: str,
) -> None:
    payload = {
        "embedding_backend": embedding_backend,
        "embedding_model": embedding_model,
        "dimension": int(vector_dim),
        "format": "chunked-v2",
        "records": chunk_records,
    }
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def resolve_input_path(primary: Path, fallback_name: str) -> Path:
    if primary.exists():
        return primary

    fallback = PROJECT_ROOT.parent / fallback_name
    if fallback.exists():
        primary.parent.mkdir(parents=True, exist_ok=True)
        primary.write_bytes(fallback.read_bytes())
        return primary

    raise FileNotFoundError(f"Missing input file: {primary} (and fallback {fallback})")


def run_ingestion(
    excel_path: Path = DEFAULT_EXCEL_PATH,
    geojson_path: Path = DEFAULT_GEOJSON_PATH,
    coordinate_excel_path: Path = DEFAULT_COORDINATE_EXCEL_PATH,
    db_path: Path = DEFAULT_DB_PATH,
    faiss_path: Path = DEFAULT_FAISS_PATH,
    metadata_path: Path = DEFAULT_METADATA_PATH,
    model_name: str = DEFAULT_MODEL_NAME,
) -> None:
    excel_path = resolve_input_path(excel_path, "GNEM updated excel.xlsx")
    geojson_path = resolve_input_path(geojson_path, "Counties_Georgia.geojson")
    coordinate_workbook = discover_coordinate_workbook(explicit_path=coordinate_excel_path)
    coordinate_df, coordinate_label = load_coordinate_enrichment(coordinate_workbook)

    xls = pd.ExcelFile(excel_path)
    sheet_name = "Data" if "Data" in xls.sheet_names else xls.sheet_names[0]
    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    df = clean_columns(df)

    if "employment" in df.columns:
        df["employment"] = (
            df["employment"]
            .astype(str)
            .str.replace(",", "", regex=False)
            .replace({"nan": None, "None": None, "": None})
        )
        df["employment"] = pd.to_numeric(df["employment"], errors="coerce")

    county_centroids = load_county_centroids(geojson_path)
    df = attach_coordinates(df, county_centroids, coordinate_df=coordinate_df)

    chunk_records = build_chunk_records(df)
    docs = [record["chunk_text"] for record in chunk_records]
    embeddings, backend_name, backend_model = create_embeddings(docs, model_name=model_name)

    write_duckdb(df, db_path)
    write_company_chunks_duckdb(chunk_records, db_path)
    write_faiss(embeddings, faiss_path)
    write_vector_metadata(
        chunk_records=chunk_records,
        vector_dim=embeddings.shape[1],
        metadata_path=metadata_path,
        embedding_backend=backend_name,
        embedding_model=backend_model,
    )

    print(f"[ingestion] Rows ingested: {len(df)}")
    print(f"[ingestion] Chunks indexed: {len(chunk_records)}")
    print(f"[ingestion] Coordinate workbook: {coordinate_label or 'not found; using existing/county fallback'}")
    print(f"[ingestion] DuckDB written: {db_path}")
    print(f"[ingestion] FAISS written: {faiss_path}")
    print(f"[ingestion] Metadata written: {metadata_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest GNEM Excel data into DuckDB + FAISS.")
    parser.add_argument("--excel", type=Path, default=DEFAULT_EXCEL_PATH, help="Path to GNEM Excel file.")
    parser.add_argument("--geojson", type=Path, default=DEFAULT_GEOJSON_PATH, help="Path to Georgia counties GeoJSON.")
    parser.add_argument(
        "--coordinates",
        type=Path,
        default=DEFAULT_COORDINATE_EXCEL_PATH,
        help="Optional path to a company-coordinate Excel workbook.",
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="Output DuckDB path.")
    parser.add_argument("--faiss", type=Path, default=DEFAULT_FAISS_PATH, help="Output FAISS index path.")
    parser.add_argument(
        "--metadata",
        type=Path,
        default=DEFAULT_METADATA_PATH,
        help="Output vector metadata JSON path.",
    )
    parser.add_argument("--model", type=str, default=DEFAULT_MODEL_NAME, help="Embedding model name.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_ingestion(
        excel_path=args.excel,
        geojson_path=args.geojson,
        coordinate_excel_path=args.coordinates,
        db_path=args.db,
        faiss_path=args.faiss,
        metadata_path=args.metadata,
        model_name=args.model,
    )
