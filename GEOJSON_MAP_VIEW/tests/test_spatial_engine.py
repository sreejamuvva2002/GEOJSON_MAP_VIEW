import json
from pathlib import Path

import duckdb
import pandas as pd

from backend.geo_utils import compute_county_for_point, load_county_geometries
from backend.spatial_engine import SpatialEngine


def _sample_geojson_path() -> Path:
    return Path(__file__).resolve().parent / "data" / "sample_counties.geojson"


def _build_spatial_db(db_path: Path) -> None:
    companies = pd.DataFrame(
        [
            {
                "company": "Alpha Inside",
                "category": "Tier 1",
                "industry_group": "Battery",
                "location": "Alpha City, Alpha County",
                "city": "Alpha City",
                "county": "Alpha",
                "computed_county": "Alpha",
                "county_key": "alpha",
                "county_id": "001",
                "county_fips": "001",
                "ev_supply_chain_role": "Battery",
                "primary_oems": "Ford",
                "employment": 100,
                "product_service": "Cells",
                "latitude": 32.6,
                "longitude": -83.6,
                "coordinate_source": "test",
                "geo_usable": True,
                "geo_validated": True,
                "geo_quality_status": "geo_usable",
            },
            {
                "company": "Beta Near Alpha",
                "category": "Tier 2",
                "industry_group": "Stamping",
                "location": "Beta City, Beta County",
                "city": "Beta City",
                "county": "Beta",
                "computed_county": "Beta",
                "county_key": "beta",
                "county_id": "003",
                "county_fips": "003",
                "ev_supply_chain_role": "Stamping",
                "primary_oems": "Ford",
                "employment": 80,
                "product_service": "Stamping",
                "latitude": 32.5,
                "longitude": -82.8,
                "coordinate_source": "test",
                "geo_usable": True,
                "geo_validated": True,
                "geo_quality_status": "geo_usable",
            },
        ]
    )
    counties = pd.DataFrame(
        [
            {"county_id": "001", "county_name": "Alpha", "county_key": "alpha", "county_fips": "001"},
            {"county_id": "003", "county_name": "Beta", "county_key": "beta", "county_fips": "003"},
        ]
    )

    with duckdb.connect(str(db_path)) as con:
        con.register("companies_df", companies)
        con.execute("CREATE TABLE companies AS SELECT * FROM companies_df")
        con.register("counties_df", counties)
        con.execute("CREATE TABLE county_dimension AS SELECT * FROM counties_df")


def test_point_in_polygon_assigns_known_county() -> None:
    county_index = load_county_geometries(_sample_geojson_path())
    county = compute_county_for_point(county_index, latitude=32.5, longitude=-83.5)
    assert county is not None
    assert county.county_name == "Alpha"


def test_projected_county_distance_orders_results(tmp_path: Path) -> None:
    db_path = tmp_path / "spatial.duckdb"
    _build_spatial_db(db_path)
    engine = SpatialEngine(db_path=db_path, geojson_path=_sample_geojson_path())

    results = engine.companies_within_miles_of_county("Alpha", miles=30)
    assert list(results["company"]) == ["Alpha Inside", "Beta Near Alpha"]
    assert float(results.iloc[0]["distance_miles"]) > 0.0
    assert float(results.iloc[0]["filter_distance_miles"]) == 0.0
    assert float(results.iloc[0]["distance_to_boundary_miles"]) > 0.0
    assert float(results.iloc[1]["distance_miles"]) > 10.0
    assert float(results.iloc[1]["distance_to_boundary_miles"]) > 10.0


def test_county_membership_returns_centroid_distance(tmp_path: Path) -> None:
    db_path = tmp_path / "spatial.duckdb"
    _build_spatial_db(db_path)
    engine = SpatialEngine(db_path=db_path, geojson_path=_sample_geojson_path())

    results = engine.companies_in_county("Alpha")
    assert list(results["company"]) == ["Alpha Inside"]
    assert str(results.iloc[0]["distance_method"]) == "county_centroid_geodesic"
    assert float(results.iloc[0]["distance_miles"]) > 0.0
    assert float(results.iloc[0]["distance_to_boundary_miles"]) > 0.0
