import json
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from backend.ingestion import run_ingestion


def test_ingestion_continues_and_marks_geo_issues(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    excel_path = tmp_path / "gnem_companies.xlsx"
    coord_path = tmp_path / "company_coordinates.xlsx"
    geojson_path = Path(__file__).resolve().parent / "data" / "sample_counties.geojson"
    metadata_path = tmp_path / "ingestion_run_metadata.json"
    db_path = tmp_path / "out.duckdb"

    source_df = pd.DataFrame(
        [
            {
                "Company": "Alpha Inside",
                "Category": "Tier 1",
                "Industry Group": "Battery",
                "Location": "Alpha City, Alpha County",
                "Primary Facility Type": "Plant",
                "EV Supply Chain Role": "Battery",
                "Primary OEMs": "Ford",
                "Supplier or Affiliation Type": "Supplier",
                "Employment": 100,
                "Product / Service": "Cells",
                "EV / Battery Relevant": "Direct",
                "Classification Method": "Supplier",
            },
            {
                "Company": "Beta Mismatch",
                "Category": "Tier 2",
                "Industry Group": "Stamping",
                "Location": "Beta City, Beta County",
                "Primary Facility Type": "Plant",
                "EV Supply Chain Role": "Stamping",
                "Primary OEMs": "Ford",
                "Supplier or Affiliation Type": "Supplier",
                "Employment": 80,
                "Product / Service": "Panels",
                "EV / Battery Relevant": "Indirect",
                "Classification Method": "Supplier",
            },
        ]
    )
    source_df.to_excel(excel_path, sheet_name="Data", index=False)

    coord_df = pd.DataFrame(
        [
            {
                "Company": "Alpha Inside",
                "Location": "Alpha City, Alpha County",
                "Address": "10 Alpha Rd, Alpha City, GA 31000",
                "Latitude": 32.5,
                "Longitude": -83.5,
            },
            {
                "Company": "Beta Mismatch",
                "Location": "Beta City, Beta County",
                "Address": "99 Alpha Rd, Alpha City, GA 31000",
                "Latitude": 32.5,
                "Longitude": -83.5,
            },
        ]
    )
    coord_df.to_excel(coord_path, index=False)

    monkeypatch.setenv("EMBEDDING_BACKEND", "hash")

    run_ingestion(
        excel_path=excel_path,
        geojson_path=geojson_path,
        coordinate_excel_path=coord_path,
        db_path=db_path,
        faiss_path=tmp_path / "out.index",
        metadata_path=tmp_path / "vector_metadata.json",
        join_audit_path=tmp_path / "coordinate_join_audit.csv",
        geo_validation_path=tmp_path / "geo_validation_report.csv",
        ingestion_metadata_path=metadata_path,
    )

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata["row_count"] == 2
    assert metadata["geo_usable_rows"] == 1
    assert metadata["geo_usable_rate"] == 0.5
    assert metadata["city_conflict_count"] == 1
    assert metadata["quality_warnings"]

    with duckdb.connect(str(db_path), read_only=True) as con:
        companies = con.execute(
            """
            SELECT company, geo_usable, geo_quality_status
            FROM companies
            ORDER BY company
            """
        ).fetchdf()
        counts = con.execute(
            """
            SELECT county_name, company_count
            FROM county_company_counts
            WHERE company_count > 0
            ORDER BY county_name
            """
        ).fetchdf()

    assert companies["company"].tolist() == ["Alpha Inside", "Beta Mismatch"]
    assert companies["geo_usable"].tolist() == [True, False]
    assert companies["geo_quality_status"].tolist() == ["geo_usable", "city_conflict"]
    assert counts["company_count"].tolist() == [1]
