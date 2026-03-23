from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional, Tuple

import duckdb
import pandas as pd
from geopy.distance import distance as geopy_distance
from geopy.geocoders import Nominatim

from backend.ingestion import load_county_centroids

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

        self.companies_df = self._load_companies()
        self.city_centroids = self._build_city_centroids(self.companies_df)
        self.county_centroids = load_county_centroids(self.geojson_path) if self.geojson_path.exists() else {}

    def _load_companies(self) -> pd.DataFrame:
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            df = con.execute(
                """
                SELECT
                    company, category, industry_group, location, city, county,
                    ev_supply_chain_role, primary_oems, employment, product_service,
                    latitude, longitude, coordinate_source
                FROM companies
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

        out: Dict[str, Tuple[float, float]] = {}
        for _, row in grouped.iterrows():
            out[str(row["city"])] = (float(row["latitude"]), float(row["longitude"]))
        return out

    def _resolve_city_coordinates(self, city_name: str) -> Optional[Tuple[float, float]]:
        key = city_name.strip().lower()
        if key in self.city_centroids:
            return self.city_centroids[key]

        if key.endswith(" county"):
            key = key.replace(" county", "").strip()
        if key in self.county_centroids:
            return self.county_centroids[key]

        mapped_county = self.CITY_TO_COUNTY.get(key)
        if mapped_county and mapped_county in self.county_centroids:
            return self.county_centroids[mapped_county]

        for city_key, coords in self.city_centroids.items():
            if key in city_key or city_key in key:
                return coords

        try:
            geocoder = Nominatim(user_agent="hybrid-geospatial-rag")
            location = geocoder.geocode(f"{city_name}, Georgia, USA", timeout=5)
            if location:
                return float(location.latitude), float(location.longitude)
        except Exception:
            return None

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
            return pd.DataFrame(columns=list(self.companies_df.columns) + ["distance_km"])

        lat, lon = coords
        return self.companies_within_radius(lat, lon, radius_km=radius_km, candidates=candidates)
