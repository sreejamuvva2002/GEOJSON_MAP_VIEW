from pathlib import Path

import duckdb
import pandas as pd

from backend.analytics_engine import AnalyticsEngine


def _build_analytics_db(db_path: Path) -> None:
    county_dimension = pd.DataFrame(
        [
            {"county_id": "001", "county_name": "Alpha", "county_key": "alpha", "county_fips": "001"},
            {"county_id": "003", "county_name": "Beta", "county_key": "beta", "county_fips": "003"},
        ]
    )
    county_company_counts = pd.DataFrame(
        [
            {"county_id": "001", "county_name": "Alpha", "county_key": "alpha", "county_fips": "001", "company_count": 1},
            {"county_id": "003", "county_name": "Beta", "county_key": "beta", "county_fips": "003", "company_count": 0},
        ]
    )
    county_role_counts = pd.DataFrame(
        [
            {"county_id": "001", "county_name": "Alpha", "county_key": "alpha", "county_fips": "001", "role_key": "battery", "role_name": "Battery", "company_count": 1},
            {"county_id": "003", "county_name": "Beta", "county_key": "beta", "county_fips": "003", "role_key": "", "role_name": "Unknown", "company_count": 0},
        ]
    )
    county_category_counts = pd.DataFrame(
        [
            {"county_id": "001", "county_name": "Alpha", "county_key": "alpha", "county_fips": "001", "category_key": "tier 1", "category_name": "Tier 1", "company_count": 1},
            {"county_id": "003", "county_name": "Beta", "county_key": "beta", "county_fips": "003", "category_key": "", "category_name": "Unknown", "company_count": 0},
        ]
    )
    companies = pd.DataFrame(
        [
            {"company": "Alpha Inside", "county_key": "alpha", "county_id": "001", "county_fips": "001", "county": "Alpha", "city": "Alpha City", "category": "Tier 1", "industry_group": "Battery", "location": "Alpha City, Alpha County", "ev_supply_chain_role": "Battery", "primary_oems": "Ford", "employment": 100, "product_service": "Cells", "latitude": 32.5, "longitude": -83.5, "coordinate_source": "test", "geo_usable": True, "geo_validated": True, "geo_quality_status": "geo_usable"}
        ]
    )

    with duckdb.connect(str(db_path)) as con:
        for table_name, frame in {
            "county_dimension": county_dimension,
            "county_company_counts": county_company_counts,
            "county_role_counts": county_role_counts,
            "county_category_counts": county_category_counts,
            "companies": companies,
        }.items():
            con.register("frame", frame)
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM frame")


def test_gap_analytics_returns_deterministic_county(tmp_path: Path) -> None:
    db_path = tmp_path / "analytics.duckdb"
    _build_analytics_db(db_path)
    engine = AnalyticsEngine(db_path=db_path)

    results = engine.counties_with_zero_matches("Tier 1")
    assert list(results["county_name"]) == ["Beta"]
