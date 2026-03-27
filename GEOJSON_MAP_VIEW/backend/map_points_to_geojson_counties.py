from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import pandas as pd
from shapely.geometry import Point

try:
    from backend.extract_address_city_county import extract_address_details
    from backend.geo_utils import CountyGeometryIndex, load_county_geometries
except ModuleNotFoundError:  # pragma: no cover - allows direct script execution
    import sys

    PROJECT_ROOT_FOR_IMPORTS = Path(__file__).resolve().parents[1]
    if str(PROJECT_ROOT_FOR_IMPORTS) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT_FOR_IMPORTS))

    from backend.extract_address_city_county import extract_address_details
    from backend.geo_utils import CountyGeometryIndex, load_county_geometries

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WORKBOOK_PATH = PROJECT_ROOT / "data" / "GNEM - Auto Landscape Lat Long Updated.xlsx"
DEFAULT_GEOJSON_PATH = PROJECT_ROOT / "data" / "Counties_Georgia.geojson"
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "data" / "company_points_to_geojson_counties.xlsx"


def compute_county_from_point(
    latitude: object,
    longitude: object,
    county_index: CountyGeometryIndex,
) -> tuple[Optional[str], Optional[str], bool]:
    lat = pd.to_numeric(pd.Series([latitude]), errors="coerce").iloc[0]
    lon = pd.to_numeric(pd.Series([longitude]), errors="coerce").iloc[0]

    if pd.isna(lat) or pd.isna(lon):
        return None, None, False
    if not (-90 <= float(lat) <= 90 and -180 <= float(lon) <= 180):
        return None, None, False

    point = Point(float(lon), float(lat))
    for county in county_index.counties:
        if county.geometry.contains(point) or county.geometry.touches(point):
            return county.county_name, county.county_id, False
    return None, None, True


def build_company_county_table(workbook_path: Path, geojson_path: Path) -> pd.DataFrame:
    df = pd.read_excel(workbook_path).rename(
        columns={
            "Company": "company_name",
            "Address": "address_raw",
            "Latitude": "latitude",
            "Longitude": "longitude",
        }
    )

    county_index = load_county_geometries(geojson_path)

    address_extracted = df["address_raw"].apply(extract_address_details).apply(pd.Series)
    result = pd.concat([df, address_extracted], axis=1)

    county_records = result.apply(
        lambda row: compute_county_from_point(
            latitude=row.get("latitude"),
            longitude=row.get("longitude"),
            county_index=county_index,
        ),
        axis=1,
        result_type="expand",
    )
    county_records.columns = [
        "computed_county_from_geojson",
        "computed_county_id_from_geojson",
        "outside_geojson_counties",
    ]
    result = pd.concat([result, county_records], axis=1)

    ordered_columns = [
        "company_name",
        "address_raw",
        "extracted_city_from_address_regex",
        "extracted_county_from_address_regex",
        "extracted_state_from_address_regex",
        "latitude",
        "longitude",
        "computed_county_from_geojson",
        "computed_county_id_from_geojson",
        "outside_geojson_counties",
    ]

    keep_columns = [col for col in ordered_columns if col in result.columns]
    return result[keep_columns].sort_values(["company_name", "address_raw"], ascending=[True, True])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Map company lat/lon points to GeoJSON counties and export the computed county per company."
    )
    parser.add_argument("--workbook", type=Path, default=DEFAULT_WORKBOOK_PATH, help="Path to the Excel workbook.")
    parser.add_argument("--geojson", type=Path, default=DEFAULT_GEOJSON_PATH, help="Path to the counties GeoJSON.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH, help="Output Excel path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    table = build_company_county_table(args.workbook, args.geojson)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    table.to_excel(args.output, index=False)
    print(f"[geojson-county-map] workbook={args.workbook}")
    print(f"[geojson-county-map] geojson={args.geojson}")
    print(f"[geojson-county-map] rows={len(table)}")
    print(f"[geojson-county-map] output={args.output}")


if __name__ == "__main__":
    main()
