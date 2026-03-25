from __future__ import annotations

from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

from backend.geo_utils import canonical_county_display_name, normalize_county_name

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "gnem.duckdb"


class AnalyticsEngine:
    def __init__(self, db_path: Path = DEFAULT_DB_PATH) -> None:
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"DuckDB file not found: {self.db_path}")

    def _query(self, sql: str, params: list | None = None) -> pd.DataFrame:
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            return con.execute(sql, params or []).fetchdf()

    def county_summary(self) -> pd.DataFrame:
        return self._query(
            """
            SELECT
                county_id,
                county_name,
                county_key,
                county_fips,
                company_count
            FROM county_company_counts
            ORDER BY county_name
            """
        )

    def counties_with_zero_matches(self, term: str) -> pd.DataFrame:
        term_key = normalize_county_name(term)
        if not term_key:
            return pd.DataFrame()

        sql = """
            WITH role_hits AS (
                SELECT county_id
                FROM county_role_counts
                WHERE role_key LIKE ?
                  AND company_count > 0
            ),
            category_hits AS (
                SELECT county_id
                FROM county_category_counts
                WHERE category_key LIKE ?
                  AND company_count > 0
            ),
            positive_hits AS (
                SELECT county_id FROM role_hits
                UNION
                SELECT county_id FROM category_hits
            )
            SELECT
                county_id,
                county_name,
                county_key,
                county_fips,
                0 AS company_count,
                ? AS analytic_term
            FROM county_dimension
            WHERE county_id NOT IN (SELECT county_id FROM positive_hits)
            ORDER BY county_name
        """
        return self._query(sql, [f"%{term_key}%", f"%{term_key}%", canonical_county_display_name(term) or term.strip()])

    def county_filter(self, county_name: str) -> pd.DataFrame:
        county_key = normalize_county_name(county_name)
        if not county_key:
            return pd.DataFrame()
        return self._query(
            """
            SELECT
                company,
                category,
                industry_group,
                location,
                city,
                county,
                county_key,
                county_fips,
                ev_supply_chain_role,
                primary_oems,
                employment,
                product_service,
                latitude,
                longitude,
                coordinate_source,
                geo_validated
            FROM companies
            WHERE county_key = ?
            ORDER BY company
            """,
            [county_key],
        )

    def top_companies_by_metric(self, metric: str, limit: int = 10) -> pd.DataFrame:
        metric_key = metric.strip().lower()
        if metric_key not in {"employment", "employees"}:
            raise ValueError(f"Unsupported analytic metric: {metric}")
        return self._query(
            """
            SELECT
                company,
                category,
                industry_group,
                location,
                city,
                county,
                county_key,
                county_fips,
                ev_supply_chain_role,
                primary_oems,
                employment AS metric_value,
                employment,
                product_service,
                latitude,
                longitude,
                coordinate_source,
                geo_validated
            FROM companies
            WHERE employment IS NOT NULL
            ORDER BY employment DESC, company
            LIMIT ?
            """,
            [int(limit)],
        )

    def available_counties(self) -> list[str]:
        df = self._query("SELECT county_name FROM county_dimension ORDER BY county_name")
        return df["county_name"].astype(str).tolist() if not df.empty else []

    def ensure_required_tables(self) -> None:
        required_tables = {
            "county_dimension",
            "county_company_counts",
            "county_role_counts",
            "county_category_counts",
        }
        tables = self._query("SHOW TABLES")
        present = set(tables.iloc[:, 0].astype(str).tolist()) if not tables.empty else set()
        missing = sorted(required_tables - present)
        if missing:
            joined = ", ".join(missing)
            raise RuntimeError(
                "Analytics tables are missing from DuckDB. Run ingestion again to build the deterministic "
                f"county analytics layer. Missing: {joined}"
            )
