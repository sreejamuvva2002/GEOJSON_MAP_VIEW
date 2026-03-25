from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional, Tuple

import duckdb
import pandas as pd
from geopy.distance import distance as geopy_distance

from backend.geo_utils import (
    PROJECTED_CRS,
    compute_point_to_county_boundary_distance_miles,
    compute_point_to_county_distance_miles as compute_projected_distance_miles,
    load_county_geometries,
    normalize_county_name,
    resolve_county_geometry,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "gnem.duckdb"
DEFAULT_GEOJSON_PATH = PROJECT_ROOT / "data" / "Counties_Georgia.geojson"


class SpatialEngine:
    CITY_TO_COUNTY = {
        "atlanta": "fulton",
        "savannah": "chatham",
        "augusta": "richmond",
        "macon": "bibb",
        "columbus": "muscogee",
        "athens": "clarke",
        "warner robins": "houston",
        "rome": "floyd",
        "valdosta": "lowndes",
        "albany": "dougherty",
        "johns creek": "fulton",
        "alpharetta": "fulton",
        "marietta": "cobb",
        "roswell": "fulton",
        "sandy springs": "fulton",
        "west point": "troup",
    }

    def __init__(
        self,
        db_path: Path = DEFAULT_DB_PATH,
        geojson_path: Path = DEFAULT_GEOJSON_PATH,
    ) -> None:
        self.db_path = Path(db_path)
        self.geojson_path = Path(geojson_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"DuckDB file not found: {self.db_path}")
        if not self.geojson_path.exists():
            raise FileNotFoundError(f"County GeoJSON file not found: {self.geojson_path}")

        self.county_index = load_county_geometries(self.geojson_path)
        self.companies_df = self._load_companies()
        self.city_centroids = self._build_city_centroids(self.companies_df)

    def _load_companies(self) -> pd.DataFrame:
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            tables = con.execute("SHOW TABLES").fetchdf().iloc[:, 0].astype(str).tolist()
            required_tables = {"companies", "county_dimension"}
            if not required_tables.issubset(set(tables)):
                missing = ", ".join(sorted(required_tables - set(tables)))
                raise RuntimeError(
                    "Deterministic geo tables are missing from DuckDB. Run ingestion again before starting the app. "
                    f"Missing: {missing}"
                )

            df = con.execute(
                """
                SELECT
                    company,
                    category,
                    industry_group,
                    location,
                    city,
                    county,
                    computed_county,
                    county_key,
                    county_id,
                    county_fips,
                    ev_supply_chain_role,
                    primary_oems,
                    employment,
                    product_service,
                    latitude,
                    longitude,
                    coordinate_source,
                    geo_usable,
                    geo_validated,
                    geo_quality_status
                FROM companies
                WHERE latitude IS NOT NULL
                  AND longitude IS NOT NULL
                  AND geo_usable = TRUE
                """
            ).fetchdf()
        return df

    @staticmethod
    def _build_city_centroids(df: pd.DataFrame) -> Dict[str, Tuple[float, float]]:
        valid = df.dropna(subset=["city", "latitude", "longitude"]).copy()
        if valid.empty:
            return {}

        grouped = (
            valid.groupby(valid["city"].str.lower().str.strip())
            .agg(latitude=("latitude", "mean"), longitude=("longitude", "mean"))
            .reset_index()
        )
        return {
            str(row["city"]): (float(row["latitude"]), float(row["longitude"]))
            for _, row in grouped.iterrows()
        }

    def _resolve_city_coordinates(self, city_name: str) -> Optional[Tuple[float, float]]:
        key = city_name.strip().lower()
        if key in self.city_centroids:
            return self.city_centroids[key]

        mapped_county = self.CITY_TO_COUNTY.get(key)
        if mapped_county:
            county = resolve_county_geometry(self.county_index, mapped_county)
            if county is not None:
                return county.centroid_latitude, county.centroid_longitude

        county = resolve_county_geometry(self.county_index, city_name)
        if county is not None:
            return county.centroid_latitude, county.centroid_longitude

        for city_key, coords in self.city_centroids.items():
            if key in city_key or city_key in key:
                return coords
        return None

    def companies_within_radius(
        self,
        lat: float,
        lon: float,
        radius_km: float,
        candidates: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        base = candidates.copy() if candidates is not None and not candidates.empty else self.companies_df.copy()
        base = base.dropna(subset=["latitude", "longitude"]).copy()
        if base.empty:
            return base

        center = (float(lat), float(lon))
        base["distance_km"] = base.apply(
            lambda row: geopy_distance(center, (float(row["latitude"]), float(row["longitude"]))).km,
            axis=1,
        )
        base["distance_miles"] = base["distance_km"] * 0.621371
        base["distance_method"] = "geodesic_point_radius"
        within = base[base["distance_km"] <= float(radius_km)].copy()
        within = within.sort_values(["distance_km", "company"], ascending=[True, True]).reset_index(drop=True)
        return within

    def companies_near_city(
        self,
        city_name: str,
        radius_km: float = 50.0,
        candidates: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        coords = self._resolve_city_coordinates(city_name)
        if not coords:
            return pd.DataFrame(columns=list(self.companies_df.columns) + ["distance_km", "distance_miles"])

        lat, lon = coords
        return self.companies_within_radius(lat, lon, radius_km=radius_km, candidates=candidates)

    def companies_in_county(
        self,
        county_name: str,
        candidates: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        county_key = normalize_county_name(county_name)
        if not county_key:
            return pd.DataFrame(columns=list(self.companies_df.columns))

        base = candidates.copy() if candidates is not None and not candidates.empty else self.companies_df.copy()
        if "county_key" not in base.columns:
            return pd.DataFrame(columns=list(base.columns))

        out = base[base["county_key"].fillna("").astype(str) == county_key].copy()
        county = resolve_county_geometry(self.county_index, county_name)
        if county is None or out.empty:
            return out.reset_index(drop=True)

        centroid = (float(county.centroid_latitude), float(county.centroid_longitude))
        out["distance_miles"] = out.apply(
            lambda row: geopy_distance(centroid, (float(row["latitude"]), float(row["longitude"]))).miles,
            axis=1,
        )
        out["distance_km"] = out["distance_miles"] * 1.60934
        out["distance_to_boundary_miles"] = out.apply(
            lambda row: compute_point_to_county_boundary_distance_miles(
                self.county_index,
                latitude=float(row["latitude"]),
                longitude=float(row["longitude"]),
                county_name=county.county_name,
            ),
            axis=1,
        )
        out["distance_to_boundary_km"] = out["distance_to_boundary_miles"] * 1.60934
        out["filter_distance_miles"] = 0.0
        out["filter_distance_km"] = 0.0
        out["distance_method"] = "county_centroid_geodesic"
        out["distance_reference"] = f"{county.county_name} County centroid"
        out["filter_distance_reference"] = f"{county.county_name} County polygon"
        return out.sort_values(["distance_miles", "company"], ascending=[True, True]).reset_index(drop=True)

    def compute_point_to_county_distance_miles(
        self,
        latitude: float,
        longitude: float,
        county_name: str,
    ) -> Optional[float]:
        return compute_projected_distance_miles(
            self.county_index,
            latitude=latitude,
            longitude=longitude,
            county_name=county_name,
        )

    def companies_within_miles_of_county(
        self,
        county_name: str,
        miles: float,
        candidates: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        county = resolve_county_geometry(self.county_index, county_name)
        if county is None:
            return pd.DataFrame(columns=list(self.companies_df.columns) + ["distance_miles", "distance_km"])

        base = candidates.copy() if candidates is not None and not candidates.empty else self.companies_df.copy()
        base = base.dropna(subset=["latitude", "longitude"]).copy()
        if base.empty:
            return base

        centroid = (float(county.centroid_latitude), float(county.centroid_longitude))
        base["filter_distance_miles"] = base.apply(
            lambda row: self.compute_point_to_county_distance_miles(
                latitude=float(row["latitude"]),
                longitude=float(row["longitude"]),
                county_name=county.county_name,
            ),
            axis=1,
        )
        base["filter_distance_km"] = base["filter_distance_miles"] * 1.60934
        base["distance_to_boundary_miles"] = base.apply(
            lambda row: compute_point_to_county_boundary_distance_miles(
                self.county_index,
                latitude=float(row["latitude"]),
                longitude=float(row["longitude"]),
                county_name=county.county_name,
            ),
            axis=1,
        )
        base["distance_to_boundary_km"] = base["distance_to_boundary_miles"] * 1.60934
        base["distance_miles"] = base.apply(
            lambda row: (
                geopy_distance(centroid, (float(row["latitude"]), float(row["longitude"]))).miles
                if float(row["filter_distance_miles"] or 0.0) == 0.0
                else float(row["filter_distance_miles"])
            ),
            axis=1,
        )
        base["distance_km"] = base["distance_miles"] * 1.60934
        base["distance_method"] = f"polygon_distance:{PROJECTED_CRS}"
        base["distance_reference"] = base["filter_distance_miles"].apply(
            lambda value: f"{county.county_name} County centroid"
            if float(value or 0.0) == 0.0
            else f"{county.county_name} County polygon"
        )
        base["filter_distance_reference"] = f"{county.county_name} County polygon"
        within = base[base["filter_distance_miles"].fillna(float("inf")) <= float(miles)].copy()
        within = within.sort_values(["filter_distance_miles", "distance_miles", "company"], ascending=[True, True, True]).reset_index(drop=True)
        return within
