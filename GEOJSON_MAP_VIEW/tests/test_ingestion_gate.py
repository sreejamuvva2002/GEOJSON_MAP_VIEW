import os
from pathlib import Path

import pandas as pd
import pytest

from backend.ingestion import IngestionGateError, run_ingestion


def test_ingestion_gate_fails_when_thresholds_exceeded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    excel_path = tmp_path / "gnem_companies.xlsx"
    coord_path = tmp_path / "company_coordinates.xlsx"
    geojson_path = Path(__file__).resolve().parent / "data" / "sample_counties.geojson"

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
                "Company": "Missing Join",
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
                "Latitude": 32.5,
                "Longitude": -83.5,
            }
        ]
    )
    coord_df.to_excel(coord_path, index=False)

    monkeypatch.setenv("EMBEDDING_BACKEND", "hash")
    monkeypatch.setenv("JOIN_MATCH_RATE_MIN", "0.99")
    monkeypatch.setenv("DUPLICATE_KEY_RATE_MAX", "0.50")
    monkeypatch.setenv("OUTSIDE_GA_RATE_MAX", "1.0")
    monkeypatch.setenv("UNASSIGNABLE_RATE_MAX", "1.0")

    with pytest.raises(IngestionGateError):
        run_ingestion(
            excel_path=excel_path,
            geojson_path=geojson_path,
            coordinate_excel_path=coord_path,
            db_path=tmp_path / "out.duckdb",
            faiss_path=tmp_path / "out.index",
            metadata_path=tmp_path / "vector_metadata.json",
        )
