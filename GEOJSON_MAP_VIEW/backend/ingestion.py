from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import duckdb
import faiss
import numpy as np
import pandas as pd

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.geo_utils import (
    PROJECTED_CRS,
    canonical_county_display_name,
    compute_county_for_point,
    file_sha256,
    load_county_geometries,
    normalize_county_name,
    stable_company_slug,
)

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
DEFAULT_JOIN_AUDIT_PATH = DATA_DIR / "coordinate_join_audit.csv"
DEFAULT_JOIN_AUDIT_JSON_PATH = DATA_DIR / "join_audit.json"
DEFAULT_GEO_VALIDATION_PATH = DATA_DIR / "geo_validation_report.csv"
DEFAULT_INGESTION_METADATA_PATH = DATA_DIR / "ingestion_run_metadata.json"

COMPANY_COLUMN_CANDIDATES = ["company", "company_name", "supplier", "supplier_name", "name"]
LOCATION_COLUMN_CANDIDATES = ["location", "facility_location", "address", "city_county", "site"]
CITY_COLUMN_CANDIDATES = ["city", "municipality", "town"]
COUNTY_COLUMN_CANDIDATES = ["county", "county_name"]
ADDRESS_COLUMN_CANDIDATES = ["address", "street_address"]
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


def extract_city_county(location: object, explicit_county: object = None) -> Tuple[Optional[str], Optional[str]]:
    location_text = normalize_cell(location)
    county_text = normalize_cell(explicit_county)

    city = None
    county = canonical_county_display_name(county_text) if county_text else None
    if not location_text:
        return city, county

    parts = [part.strip() for part in location_text.split(",") if part.strip()]
    if parts:
        first = parts[0]
        if "county" not in first.lower():
            city = first.title()

    if county is None:
        county_match = re.search(r"([A-Za-z][A-Za-z\s\-'.&]+?)\s+County\b", location_text, flags=re.IGNORECASE)
        if county_match:
            county = canonical_county_display_name(county_match.group(1))
        elif len(parts) > 1 and "county" in parts[1].lower():
            county = canonical_county_display_name(parts[1])

    return city, county


def extract_address_city(address: object) -> Optional[str]:
    text = normalize_cell(address)
    if not text:
        return None
    parts = [part.strip() for part in text.split(",") if part.strip()]
    if len(parts) < 2:
        return None
    return parts[-2].title()


def normalize_city_key(value: object) -> Optional[str]:
    key = normalize_match_key(value)
    return key or None


def load_county_polygons(geojson_path: Path):
    return load_county_geometries(geojson_path)


def load_county_centroids(geojson_path: Path) -> Dict[str, Tuple[float, float]]:
    county_index = load_county_polygons(geojson_path)
    return county_index.centroid_lookup


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
        "address": ADDRESS_COLUMN_CANDIDATES,
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
        "GNEM - Auto Landscape Lat Long Updated.xlsx",
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
        for optional_key in ("location", "city", "county", "address"):
            optional_col = detected.get(optional_key)
            if optional_col:
                rename_map[optional_col] = f"coord_{optional_key}"

        coords = full_df.rename(columns=rename_map)
        keep_cols = [col for col in coords.columns if col.startswith("coord_")]
        coords = coords[keep_cols].copy()
        coords["coord_company_key"] = coords["coord_company"].apply(normalize_match_key)
        coords["coord_location_key"] = (
            coords["coord_location"].apply(normalize_match_key) if "coord_location" in coords.columns else ""
        )
        coords["coord_city_key"] = coords["coord_city"].apply(normalize_match_key) if "coord_city" in coords.columns else ""
        coords["coord_county_key"] = (
            coords["coord_county"].apply(normalize_county_name) if "coord_county" in coords.columns else None
        )
        coords["coord_address_key"] = (
            coords["coord_address"].apply(normalize_match_key) if "coord_address" in coords.columns else ""
        )
        coords["coord_exact_join_key"] = coords.apply(
            lambda row: f"{row['coord_company_key']}||{row['coord_location_key']}" if row["coord_location_key"] else "",
            axis=1,
        )

        coords["coord_latitude"] = pd.to_numeric(coords["coord_latitude"], errors="coerce")
        coords["coord_longitude"] = pd.to_numeric(coords["coord_longitude"], errors="coerce")
        coords = coords.dropna(subset=["coord_latitude", "coord_longitude"]).copy()
        coords = coords[
            coords["coord_latitude"].between(-90, 90) & coords["coord_longitude"].between(-180, 180)
        ].copy()
        if coords.empty:
            continue

        coords["coordinate_source_file"] = workbook_path.name
        coords["coordinate_source_sheet"] = sheet_name
        return coords.reset_index(drop=True), f"{workbook_path.name}::{sheet_name}"

    return pd.DataFrame(), None


def _build_unique_lookup(df: pd.DataFrame, key_column: str) -> Tuple[Dict[str, dict], set[str]]:
    if df.empty or key_column not in df.columns:
        return {}, set()

    keyed = df[df[key_column].fillna("").astype(str).str.strip() != ""].copy()
    if keyed.empty:
        return {}, set()

    grouped = keyed.groupby(key_column, dropna=False)
    duplicate_keys = {str(key) for key, group in grouped if len(group) > 1}
    unique_lookup: Dict[str, dict] = {}
    for key, group in grouped:
        key_text = str(key)
        if key_text in duplicate_keys:
            continue
        unique_lookup[key_text] = group.iloc[0].to_dict()
    return unique_lookup, duplicate_keys


def _coordinate_duplicate_rate(coordinate_df: pd.DataFrame) -> Tuple[float, Dict[str, int]]:
    if coordinate_df.empty:
        return 0.0, {
            "exact_duplicate_keys": 0,
            "exact_unique_keys": 0,
            "company_duplicate_keys": 0,
            "company_unique_keys": 0,
        }

    exact_series = coordinate_df["coord_exact_join_key"].fillna("").astype(str)
    exact_series = exact_series[exact_series != ""]
    company_series = coordinate_df["coord_company_key"].fillna("").astype(str)
    company_series = company_series[company_series != ""]

    exact_duplicate_keys = int(exact_series.value_counts().gt(1).sum()) if not exact_series.empty else 0
    exact_unique_keys = int(exact_series.nunique()) if not exact_series.empty else 0
    company_duplicate_keys = int(company_series.value_counts().gt(1).sum()) if not company_series.empty else 0
    company_unique_keys = int(company_series.nunique()) if not company_series.empty else 0

    denominator = max(1, exact_unique_keys + company_unique_keys)
    duplicate_key_rate = (exact_duplicate_keys + company_duplicate_keys) / denominator
    return duplicate_key_rate, {
        "exact_duplicate_keys": exact_duplicate_keys,
        "exact_unique_keys": exact_unique_keys,
        "company_duplicate_keys": company_duplicate_keys,
        "company_unique_keys": company_unique_keys,
    }


def _join_audit_summary(
    *,
    join_audit_df: pd.DataFrame,
    usable_coordinates: pd.Series,
    geo_usable_flags: pd.Series,
    outside_flags: pd.Series,
    unassignable_flags: pd.Series,
    mismatch_flags: pd.Series,
    city_conflict_flags: pd.Series,
    county_field_trusted: bool,
    duplicate_key_rate: float,
    duplicate_key_counts: Dict[str, int],
    county_index,
    join_key_used: str,
) -> Dict[str, object]:
    total_rows = max(1, len(join_audit_df))
    coordinate_join_mask = join_audit_df["join_status"].isin({"matched_exact", "matched_company_fallback"})
    matched_rows = int(coordinate_join_mask.sum())

    usable_count = int(usable_coordinates.sum())
    geo_usable_count = int(geo_usable_flags.sum())
    outside_count = int(outside_flags.sum())
    unassignable_count = int(unassignable_flags.sum())
    mismatch_count = int(mismatch_flags.sum())
    city_conflict_count = int(city_conflict_flags.sum())
    mismatch_denominator = int((join_audit_df["existing_county"].fillna("") != "").sum())

    return {
        "COUNTY_FIELD_TRUSTED": county_field_trusted,
        "join_key_used": join_key_used,
        "row_count": int(len(join_audit_df)),
        "join_match_rate": matched_rows / total_rows,
        "join_match_count": matched_rows,
        "duplicate_key_rate": duplicate_key_rate,
        "duplicate_key_counts": duplicate_key_counts,
        "usable_coordinate_rows": usable_count,
        "geo_usable_rows": geo_usable_count,
        "geo_usable_rate": (geo_usable_count / total_rows) if total_rows else 0.0,
        "outside_ga_rate": (outside_count / usable_count) if usable_count else 0.0,
        "outside_ga_count": outside_count,
        "unassignable_rate": (unassignable_count / usable_count) if usable_count else 0.0,
        "unassignable_count": unassignable_count,
        "mismatch_rate": (mismatch_count / mismatch_denominator) if mismatch_denominator else 0.0,
        "mismatch_count": mismatch_count,
        "mismatch_denominator": mismatch_denominator,
        "city_conflict_count": city_conflict_count,
        "city_conflict_rate": (city_conflict_count / total_rows) if total_rows else 0.0,
        "county_polygon_repair_count": county_index.repair_count,
        "county_polygon_repair_counties": county_index.repaired_counties,
        "county_geometry_hash": county_index.geometry_hash,
        "configured_crs": PROJECTED_CRS,
    }


def attach_coordinates(
    df: pd.DataFrame,
    county_index,
    coordinate_df: Optional[pd.DataFrame] = None,
    county_field_trusted: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, object]]:
    out = df.copy()
    if "latitude" in out.columns:
        out["latitude"] = pd.to_numeric(out["latitude"], errors="coerce")
    else:
        out["latitude"] = np.nan
    if "longitude" in out.columns:
        out["longitude"] = pd.to_numeric(out["longitude"], errors="coerce")
    else:
        out["longitude"] = np.nan

    source_cities: List[Optional[str]] = []
    source_counties: List[Optional[str]] = []
    join_rows: List[Dict[str, object]] = []
    computed_counties: List[Optional[str]] = []
    county_keys: List[Optional[str]] = []
    county_ids: List[Optional[str]] = []
    county_fips: List[Optional[str]] = []
    latitude_values: List[Optional[float]] = []
    longitude_values: List[Optional[float]] = []
    coordinate_sources: List[str] = []
    outside_flags: List[bool] = []
    unassignable_flags: List[bool] = []
    mismatch_flags: List[bool] = []
    city_conflict_flags: List[bool] = []
    quality_statuses: List[str] = []
    geo_validated_flags: List[bool] = []
    geo_usable_flags: List[bool] = []

    exact_lookup: Dict[str, dict] = {}
    exact_duplicate_keys: set[str] = set()
    company_lookup: Dict[str, dict] = {}
    company_duplicate_keys: set[str] = set()
    duplicate_key_rate = 0.0
    duplicate_key_counts: Dict[str, int] = {
        "exact_duplicate_keys": 0,
        "exact_unique_keys": 0,
        "company_duplicate_keys": 0,
        "company_unique_keys": 0,
    }

    if coordinate_df is not None and not coordinate_df.empty:
        exact_lookup, exact_duplicate_keys = _build_unique_lookup(coordinate_df, "coord_exact_join_key")
        company_lookup, company_duplicate_keys = _build_unique_lookup(coordinate_df, "coord_company_key")
        duplicate_key_rate, duplicate_key_counts = _coordinate_duplicate_rate(coordinate_df)

    for row_idx, (_, row) in enumerate(out.iterrows()):
        explicit_county = row.get("county")
        city, existing_county = extract_city_county(row.get("location"), explicit_county=explicit_county)
        source_cities.append(city)
        source_counties.append(existing_county)

        company_name = normalize_cell(row.get("company"))
        location_text = normalize_cell(row.get("location"))
        company_key = normalize_match_key(company_name)
        location_key = normalize_match_key(location_text)
        exact_join_key = f"{company_key}||{location_key}" if location_key else ""
        source_city_key = normalize_city_key(city)

        lat = _safe_float(row.get("latitude"))
        lon = _safe_float(row.get("longitude"))
        join_status = "source_coordinates"
        join_method = "source"
        join_record = None
        coordinate_source = "source_excel" if lat is not None and lon is not None else "missing"

        if lat is None or lon is None:
            join_status = "not_attempted"
            join_method = "missing"
            if exact_join_key and exact_join_key in exact_lookup:
                join_record = exact_lookup[exact_join_key]
                lat = _safe_float(join_record.get("coord_latitude"))
                lon = _safe_float(join_record.get("coord_longitude"))
                join_status = "matched_exact"
                join_method = "company+location"
            elif exact_join_key and exact_join_key in exact_duplicate_keys:
                join_status = "duplicate_exact_key"
                join_method = "company+location"
            elif company_key and company_key in company_lookup:
                join_record = company_lookup[company_key]
                lat = _safe_float(join_record.get("coord_latitude"))
                lon = _safe_float(join_record.get("coord_longitude"))
                join_status = "matched_company_fallback"
                join_method = "company"
            elif company_key and company_key in company_duplicate_keys:
                join_status = "duplicate_company_key"
                join_method = "company"
            else:
                join_status = "no_join_match"
                join_method = "company+location_then_company"

            if lat is not None and lon is not None:
                coordinate_source = (
                    f"coordinates_excel:{normalize_cell(join_record.get('coordinate_source_file')) or 'external'}"
                    if join_record
                    else "source_excel"
                )

        usable_coordinates = lat is not None and lon is not None and -90 <= lat <= 90 and -180 <= lon <= 180
        latitude_values.append(lat if usable_coordinates else None)
        longitude_values.append(lon if usable_coordinates else None)

        computed_county = None
        county_key = None
        county_id = None
        county_fips_value = None
        outside_flag = False
        unassignable_flag = False
        city_conflict = False
        geo_usable = False
        quality_status = "missing_coordinates"

        coord_declared_city = None
        coord_location_city = None
        address_city = None
        coord_declared_county = None
        coord_location_county = None

        if usable_coordinates:
            county_match = compute_county_for_point(county_index, latitude=lat, longitude=lon)
            if county_match is None:
                outside_flag = True
                unassignable_flag = True
                quality_status = "outside_reference_geometry"
            else:
                computed_county = county_match.county_name
                county_key = county_match.county_key
                county_id = county_match.county_id
                county_fips_value = county_match.county_fips

        existing_county_key = normalize_county_name(existing_county)
        if join_record:
            coord_declared_city = normalize_cell(join_record.get("coord_city")) or None
            coord_location_city, coord_location_county = extract_city_county(
                join_record.get("coord_location"),
                explicit_county=join_record.get("coord_county"),
            )
            address_city = extract_address_city(join_record.get("coord_address"))
            coord_declared_county = canonical_county_display_name(join_record.get("coord_county")) or None

        strong_city_keys = {
            key
            for key in [
                normalize_city_key(coord_declared_city),
                normalize_city_key(address_city),
            ]
            if key
        }
        weak_city_keys = {
            key
            for key in [
                normalize_city_key(coord_location_city),
            ]
            if key
        }

        county_supported = not existing_county_key or (county_key is not None and existing_county_key == county_key)
        if source_city_key:
            if strong_city_keys:
                city_supported = source_city_key in strong_city_keys
            elif weak_city_keys and join_status == "matched_exact":
                city_supported = source_city_key in weak_city_keys
            elif join_status in {"source_coordinates", "matched_exact"} and existing_county_key and county_supported:
                city_supported = True
            else:
                city_supported = False
        else:
            city_supported = True

        mismatch_flag = bool(
            county_field_trusted
            and existing_county_key
            and county_key
            and existing_county_key != county_key
        )
        city_conflict = bool(source_city_key and strong_city_keys and source_city_key not in strong_city_keys)

        if not usable_coordinates:
            quality_status = join_status if join_status != "source_coordinates" else "missing_coordinates"
        elif county_key is None:
            quality_status = "outside_reference_geometry"
        elif mismatch_flag:
            quality_status = "county_conflict"
        elif city_conflict:
            quality_status = "city_conflict"
        else:
            if join_status == "source_coordinates":
                geo_usable = True
            elif join_status == "matched_exact":
                geo_usable = county_supported and city_supported
            elif join_status == "matched_company_fallback":
                geo_usable = county_supported and city_supported and bool(existing_county_key or source_city_key)
            else:
                geo_usable = False

            if geo_usable:
                quality_status = "geo_usable"
            elif join_status == "matched_company_fallback":
                quality_status = "company_fallback_unverified"
            else:
                quality_status = "insufficient_location_consistency"

        coordinate_sources.append(coordinate_source if usable_coordinates else "missing")
        computed_counties.append(computed_county)
        county_keys.append(county_key if geo_usable and county_key else existing_county_key)
        county_ids.append(county_id if geo_usable else None)
        county_fips.append(county_fips_value if geo_usable else None)
        outside_flags.append(outside_flag)
        unassignable_flags.append(unassignable_flag)
        mismatch_flags.append(mismatch_flag)
        city_conflict_flags.append(city_conflict)
        quality_statuses.append(quality_status)
        geo_validated_flags.append(geo_usable)
        geo_usable_flags.append(geo_usable)

        join_rows.append(
            {
                "row_index": row_idx,
                "company": company_name,
                "location": location_text,
                "source_city": city,
                "existing_county": existing_county,
                "computed_county": computed_county,
                "join_key_used": join_method,
                "exact_join_key": exact_join_key,
                "company_key": company_key,
                "join_status": join_status,
                "coordinate_source_file": normalize_cell(join_record.get("coordinate_source_file")) if join_record else "",
                "coordinate_source_sheet": normalize_cell(join_record.get("coordinate_source_sheet")) if join_record else "",
                "coordinate_company": normalize_cell(join_record.get("coord_company")) if join_record else "",
                "coordinate_location": normalize_cell(join_record.get("coord_location")) if join_record else "",
                "coordinate_address": normalize_cell(join_record.get("coord_address")) if join_record else "",
                "coordinate_city": coord_declared_city or "",
                "coordinate_location_city": coord_location_city or "",
                "coordinate_address_city": address_city or "",
                "coordinate_county": coord_declared_county or "",
                "coordinate_location_county": coord_location_county or "",
                "latitude": lat if usable_coordinates else None,
                "longitude": lon if usable_coordinates else None,
                "outside_georgia_polygon": outside_flag,
                "unassignable_county": unassignable_flag,
                "county_mismatch": mismatch_flag,
                "city_conflict": city_conflict,
                "geo_usable": geo_usable,
                "geo_quality_status": quality_status,
            }
        )

    out["city"] = source_cities
    out["existing_county"] = source_counties
    out["computed_county"] = computed_counties
    out["county"] = [
        computed if geo_usable and computed else existing
        for computed, existing, geo_usable in zip(computed_counties, source_counties, geo_usable_flags)
    ]
    out["county_key"] = county_keys
    out["county_id"] = county_ids
    out["county_fips"] = county_fips
    out["latitude"] = latitude_values
    out["longitude"] = longitude_values
    out["coordinate_source"] = coordinate_sources
    out["outside_georgia_polygon"] = outside_flags
    out["unassignable_county"] = unassignable_flags
    out["county_mismatch"] = mismatch_flags
    out["city_conflict"] = city_conflict_flags
    out["geo_quality_status"] = quality_statuses
    out["geo_usable"] = geo_usable_flags
    out["geo_validated"] = geo_validated_flags

    join_audit_df = pd.DataFrame(join_rows)
    usable_coordinates_series = out["latitude"].notna() & out["longitude"].notna()
    summary = _join_audit_summary(
        join_audit_df=join_audit_df,
        usable_coordinates=usable_coordinates_series,
        geo_usable_flags=out["geo_usable"],
        outside_flags=out["outside_georgia_polygon"],
        unassignable_flags=out["unassignable_county"],
        mismatch_flags=out["county_mismatch"],
        city_conflict_flags=out["city_conflict"],
        county_field_trusted=county_field_trusted,
        duplicate_key_rate=duplicate_key_rate,
        duplicate_key_counts=duplicate_key_counts,
        county_index=county_index,
        join_key_used="company+location exact match, then company fallback",
    )
    return out, join_audit_df, summary


def _company_slug(company: str, fallback_idx: int) -> str:
    return stable_company_slug(company, fallback=f"company-{fallback_idx}")


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
        computed_county = normalize_cell(row.get("computed_county"))
        employment = _safe_float(row.get("employment"))
        latitude = _safe_float(row.get("latitude"))
        longitude = _safe_float(row.get("longitude"))
        coordinate_source = normalize_cell(row.get("coordinate_source"))
        county_fips = normalize_cell(row.get("county_fips"))
        geo_usable = bool(row.get("geo_usable", row.get("geo_validated")))
        geo_quality_status = normalize_cell(row.get("geo_quality_status"))

        base = {
            "company": company,
            "category": category,
            "industry_group": industry_group,
            "location": location,
            "city": city,
            "county": county,
            "computed_county": computed_county,
            "county_fips": county_fips,
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
            "geo_usable": geo_usable,
            "geo_validated": geo_usable,
            "geo_quality_status": geo_quality_status,
            "row_index": int(row_idx),
            "source_dataset": "gnem_companies.xlsx",
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
                f"Computed County: {computed_county or 'unknown'}\n"
                f"County FIPS: {county_fips or 'unknown'}\n"
                f"Latitude: {latitude if latitude is not None else 'unknown'}\n"
                f"Longitude: {longitude if longitude is not None else 'unknown'}\n"
                f"Coordinate Source: {coordinate_source or 'unknown'}\n"
                f"Geo Usable: {geo_usable}\n"
                f"Geo Quality Status: {geo_quality_status or 'unknown'}\n"
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
    backend_pref = os.getenv("EMBEDDING_BACKEND", "hash").strip().lower()
    if backend_pref == "hash":
        embeddings = np.vstack([_hash_embed_one(doc) for doc in docs]).astype(np.float32)
        return embeddings, "hash", "hashed-token-384"

    if backend_pref in {"sentence-transformers", "sbert", "sentence"}:
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

    raise ValueError(
        "Unsupported EMBEDDING_BACKEND. Use 'hash' or 'sentence-transformers' for deterministic indexing."
    )


def write_duckdb(df: pd.DataFrame, db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as con:
        con.register("df_companies", df)
        con.execute("CREATE OR REPLACE TABLE companies AS SELECT * FROM df_companies")


def write_company_chunks_duckdb(chunk_records: List[dict], db_path: Path) -> None:
    with duckdb.connect(str(db_path)) as con:
        chunk_df = pd.DataFrame(chunk_records)
        con.register("df_company_chunks", chunk_df)
        con.execute("CREATE OR REPLACE TABLE company_chunks AS SELECT * FROM df_company_chunks")


def write_county_tables(db_path: Path, county_index, issue_df: pd.DataFrame) -> None:
    county_rows = [
        {
            "county_id": county.county_id,
            "county_name": county.county_name,
            "county_key": county.county_key,
            "county_fips": county.county_fips,
            "geoid": county.geoid,
            "centroid_latitude": county.centroid_latitude,
            "centroid_longitude": county.centroid_longitude,
            "repair_applied": county.repair_applied,
            "repair_method": county.repair_method,
        }
        for county in county_index.counties
    ]
    county_df = pd.DataFrame(county_rows)

    with duckdb.connect(str(db_path)) as con:
        con.register("df_counties", county_df)
        con.execute("CREATE OR REPLACE TABLE county_dimension AS SELECT * FROM df_counties")

        con.register("df_geo_issues", issue_df if not issue_df.empty else pd.DataFrame({"placeholder": []}))
        if issue_df.empty:
            con.execute("CREATE OR REPLACE TABLE geo_evaluation_issues AS SELECT * FROM df_geo_issues LIMIT 0")
        else:
            con.execute("CREATE OR REPLACE TABLE geo_evaluation_issues AS SELECT * FROM df_geo_issues")

        con.execute(
            """
            CREATE OR REPLACE TABLE county_company_counts AS
            SELECT
                d.county_id,
                d.county_name,
                d.county_key,
                d.county_fips,
                COUNT(DISTINCT c.company) AS company_count
            FROM county_dimension d
            LEFT JOIN companies c
              ON c.geo_usable = TRUE
             AND c.county_id = d.county_id
            GROUP BY 1, 2, 3, 4
            ORDER BY d.county_name
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE county_role_counts AS
            SELECT
                d.county_id,
                d.county_name,
                d.county_key,
                d.county_fips,
                LOWER(COALESCE(c.ev_supply_chain_role, '')) AS role_key,
                COALESCE(c.ev_supply_chain_role, 'Unknown') AS role_name,
                COUNT(*) AS company_count
            FROM county_dimension d
            LEFT JOIN companies c
              ON c.geo_usable = TRUE
             AND c.county_id = d.county_id
            GROUP BY 1, 2, 3, 4, 5, 6
            ORDER BY d.county_name, role_name
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE county_category_counts AS
            SELECT
                d.county_id,
                d.county_name,
                d.county_key,
                d.county_fips,
                LOWER(COALESCE(c.category, '')) AS category_key,
                COALESCE(c.category, 'Unknown') AS category_name,
                COUNT(*) AS company_count
            FROM county_dimension d
            LEFT JOIN companies c
              ON c.geo_usable = TRUE
             AND c.county_id = d.county_id
            GROUP BY 1, 2, 3, 4, 5, 6
            ORDER BY d.county_name, category_name
            """
        )


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
        "format": "chunked-v3",
        "records": chunk_records,
    }
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_audit_artifacts(
    *,
    join_audit_df: pd.DataFrame,
    validation_df: pd.DataFrame,
    audit_summary: Dict[str, object],
    join_audit_path: Path = DEFAULT_JOIN_AUDIT_PATH,
    join_audit_json_path: Path = DEFAULT_JOIN_AUDIT_JSON_PATH,
    geo_validation_path: Path = DEFAULT_GEO_VALIDATION_PATH,
) -> None:
    join_audit_path.parent.mkdir(parents=True, exist_ok=True)
    join_audit_df.to_csv(join_audit_path, index=False)
    validation_df.to_csv(geo_validation_path, index=False)
    join_audit_json_path.write_text(json.dumps(audit_summary, indent=2, default=str), encoding="utf-8")


def write_ingestion_metadata(
    *,
    metadata_path: Path,
    summary: Dict[str, object],
    excel_path: Path,
    geojson_path: Path,
    coordinate_workbook: Optional[Path],
    embedding_backend: Optional[str],
    embedding_model: Optional[str],
) -> None:
    payload = {
        **summary,
        "excel_path": str(excel_path),
        "geojson_path": str(geojson_path),
        "coordinate_workbook": str(coordinate_workbook) if coordinate_workbook else None,
        "excel_file_hash": file_sha256(excel_path),
        "geojson_file_hash": file_sha256(geojson_path),
        "coordinate_workbook_hash": file_sha256(coordinate_workbook) if coordinate_workbook else None,
        "embedding_backend": embedding_backend,
        "embedding_model": embedding_model,
    }
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def resolve_input_path(primary: Path, fallback_name: str) -> Path:
    if primary.exists():
        return primary

    fallback = PROJECT_ROOT.parent / fallback_name
    if fallback.exists():
        primary.parent.mkdir(parents=True, exist_ok=True)
        primary.write_bytes(fallback.read_bytes())
        return primary

    raise FileNotFoundError(f"Missing input file: {primary} (and fallback {fallback})")


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _gate_thresholds() -> Dict[str, float]:
    return {
        "join_match_rate_min": float(os.getenv("JOIN_MATCH_RATE_MIN", "0.99")),
        "duplicate_key_rate_max": float(os.getenv("DUPLICATE_KEY_RATE_MAX", "0.005")),
        "outside_ga_rate_max": float(os.getenv("OUTSIDE_GA_RATE_MAX", "0.01")),
        "unassignable_rate_max": float(os.getenv("UNASSIGNABLE_RATE_MAX", "0.005")),
        "mismatch_rate_max": float(os.getenv("MISMATCH_RATE_MAX", "0.05")),
        "geo_usable_rate_min": float(os.getenv("GEO_USABLE_RATE_MIN", "0.70")),
        "city_conflict_rate_max": float(os.getenv("CITY_CONFLICT_RATE_MAX", "0.10")),
    }


def _collect_quality_warnings(summary: Dict[str, object], thresholds: Dict[str, float]) -> List[str]:
    warnings: List[str] = []

    if float(summary["join_match_rate"]) < thresholds["join_match_rate_min"]:
        warnings.append(
            f"join_match_rate={summary['join_match_rate']:.4f} below threshold {thresholds['join_match_rate_min']:.4f}"
        )
    if float(summary["duplicate_key_rate"]) > thresholds["duplicate_key_rate_max"]:
        warnings.append(
            f"duplicate_key_rate={summary['duplicate_key_rate']:.4f} above threshold {thresholds['duplicate_key_rate_max']:.4f}"
        )
    if float(summary["outside_ga_rate"]) > thresholds["outside_ga_rate_max"]:
        warnings.append(
            f"outside_ga_rate={summary['outside_ga_rate']:.4f} above threshold {thresholds['outside_ga_rate_max']:.4f}"
        )
    if float(summary["unassignable_rate"]) > thresholds["unassignable_rate_max"]:
        warnings.append(
            f"unassignable_rate={summary['unassignable_rate']:.4f} above threshold {thresholds['unassignable_rate_max']:.4f}"
        )
    if float(summary.get("geo_usable_rate", 0.0)) < thresholds["geo_usable_rate_min"]:
        warnings.append(
            f"geo_usable_rate={summary.get('geo_usable_rate', 0.0):.4f} below threshold {thresholds['geo_usable_rate_min']:.4f}"
        )
    if float(summary.get("city_conflict_rate", 0.0)) > thresholds["city_conflict_rate_max"]:
        warnings.append(
            f"city_conflict_rate={summary.get('city_conflict_rate', 0.0):.4f} above threshold {thresholds['city_conflict_rate_max']:.4f}"
        )

    county_field_trusted = bool(summary["COUNTY_FIELD_TRUSTED"])
    mismatch_rate = float(summary["mismatch_rate"])
    if county_field_trusted and mismatch_rate > thresholds["mismatch_rate_max"]:
        warnings.append(
            f"mismatch_rate={mismatch_rate:.4f} above threshold {thresholds['mismatch_rate_max']:.4f}"
        )

    return warnings


def run_ingestion(
    excel_path: Path = DEFAULT_EXCEL_PATH,
    geojson_path: Path = DEFAULT_GEOJSON_PATH,
    coordinate_excel_path: Path = DEFAULT_COORDINATE_EXCEL_PATH,
    db_path: Path = DEFAULT_DB_PATH,
    faiss_path: Path = DEFAULT_FAISS_PATH,
    metadata_path: Path = DEFAULT_METADATA_PATH,
    join_audit_path: Path = DEFAULT_JOIN_AUDIT_PATH,
    geo_validation_path: Path = DEFAULT_GEO_VALIDATION_PATH,
    ingestion_metadata_path: Path = DEFAULT_INGESTION_METADATA_PATH,
    model_name: str = DEFAULT_MODEL_NAME,
) -> None:
    excel_path = resolve_input_path(excel_path, "GNEM updated excel.xlsx")
    geojson_path = resolve_input_path(geojson_path, "Counties_Georgia.geojson")
    coordinate_workbook = discover_coordinate_workbook(explicit_path=coordinate_excel_path)
    coordinate_df, coordinate_label = load_coordinate_enrichment(coordinate_workbook)
    county_field_trusted = _env_bool("COUNTY_FIELD_TRUSTED", False)
    thresholds = _gate_thresholds()

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

    county_index = load_county_polygons(geojson_path)
    attached_df, join_audit_df, summary = attach_coordinates(
        df=df,
        county_index=county_index,
        coordinate_df=coordinate_df,
        county_field_trusted=county_field_trusted,
    )
    summary["thresholds"] = thresholds
    summary["coordinate_workbook_label"] = coordinate_label
    summary["coordinate_workbook_found"] = coordinate_workbook is not None
    summary["quality_warnings"] = _collect_quality_warnings(summary, thresholds)

    write_audit_artifacts(
        join_audit_df=join_audit_df,
        validation_df=attached_df,
        audit_summary=summary,
        join_audit_path=join_audit_path,
        geo_validation_path=geo_validation_path,
    )
    write_ingestion_metadata(
        metadata_path=ingestion_metadata_path,
        summary=summary,
        excel_path=excel_path,
        geojson_path=geojson_path,
        coordinate_workbook=coordinate_workbook,
        embedding_backend=None,
        embedding_model=None,
    )

    companies_df = attached_df.copy().reset_index(drop=True)
    issue_df = attached_df[~attached_df["geo_usable"]].copy().reset_index(drop=True)
    chunk_records = build_chunk_records(companies_df)
    docs = [record["chunk_text"] for record in chunk_records]
    embeddings, backend_name, backend_model = create_embeddings(docs, model_name=model_name)

    write_duckdb(companies_df, db_path)
    write_company_chunks_duckdb(chunk_records, db_path)
    write_county_tables(db_path, county_index=county_index, issue_df=issue_df)
    write_faiss(embeddings, faiss_path)
    write_vector_metadata(
        chunk_records=chunk_records,
        vector_dim=embeddings.shape[1],
        metadata_path=metadata_path,
        embedding_backend=backend_name,
        embedding_model=backend_model,
    )
    write_ingestion_metadata(
        metadata_path=ingestion_metadata_path,
        summary=summary,
        excel_path=excel_path,
        geojson_path=geojson_path,
        coordinate_workbook=coordinate_workbook,
        embedding_backend=backend_name,
        embedding_model=backend_model,
    )

    print(f"[ingestion] Rows ingested: {len(companies_df)}")
    print(f"[ingestion] Rows with geo-usable coordinates: {int(companies_df['geo_usable'].sum())}")
    print(f"[ingestion] Rows with geo issues: {len(issue_df)}")
    print(f"[ingestion] Chunks indexed: {len(chunk_records)}")
    print(f"[ingestion] Coordinate workbook: {coordinate_label or 'not found'}")
    print(f"[ingestion] COUNTY_FIELD_TRUSTED={county_field_trusted}")
    print(f"[ingestion] County polygon repairs: {county_index.repair_count}")
    print(f"[ingestion] County geometry hash: {county_index.geometry_hash}")
    print(f"[ingestion] Join match rate: {summary['join_match_rate']:.4f}")
    print(f"[ingestion] Geo usable rate: {summary['geo_usable_rate']:.4f}")
    print(f"[ingestion] Duplicate key rate: {summary['duplicate_key_rate']:.4f}")
    print(f"[ingestion] Outside GA rate: {summary['outside_ga_rate']:.4f}")
    print(f"[ingestion] Unassignable rate: {summary['unassignable_rate']:.4f}")
    print(f"[ingestion] Mismatch rate: {summary['mismatch_rate']:.4f}")
    print(f"[ingestion] City conflict rate: {summary['city_conflict_rate']:.4f}")
    if summary["quality_warnings"]:
        print("[ingestion] Quality warnings:")
        for warning in summary["quality_warnings"]:
            print(f"[ingestion]  - {warning}")
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
    parser.add_argument(
        "--join-audit",
        type=Path,
        default=DEFAULT_JOIN_AUDIT_PATH,
        help="Output coordinate join audit CSV path.",
    )
    parser.add_argument(
        "--geo-validation",
        type=Path,
        default=DEFAULT_GEO_VALIDATION_PATH,
        help="Output geo validation CSV path.",
    )
    parser.add_argument(
        "--ingestion-metadata",
        type=Path,
        default=DEFAULT_INGESTION_METADATA_PATH,
        help="Output ingestion metadata JSON path.",
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
        join_audit_path=args.join_audit,
        geo_validation_path=args.geo_validation,
        ingestion_metadata_path=args.ingestion_metadata,
        model_name=args.model,
    )
