from __future__ import annotations

import math
from typing import Dict, List, Optional

import pandas as pd
from geopy.distance import distance as geopy_distance

from backend.geo_utils import normalize_county_name

DEFAULT_MAP_VIEW = {
    "latitude": 32.75,
    "longitude": -83.4,
    "zoom": 6.3,
    "pitch": 0,
}


def filtered_records(records: List[Dict], selected_county: Optional[str]) -> pd.DataFrame:
    df = pd.DataFrame(records).copy()
    if df.empty or not selected_county or selected_county == "All Counties":
        return df

    county_key = normalize_county_name(selected_county)
    if "county_key" in df.columns:
        return df[df["county_key"].fillna("").astype(str) == county_key].copy()
    if "county" in df.columns:
        return df[df["county"].fillna("").apply(normalize_county_name) == county_key].copy()
    return df


def map_points_df(records: List[Dict], selected_county: Optional[str]) -> pd.DataFrame:
    filtered_df = filtered_records(records, selected_county)
    point_df = filtered_df.copy() if not filtered_df.empty else pd.DataFrame(records).copy()
    if point_df.empty or not {"latitude", "longitude"}.issubset(point_df.columns):
        return pd.DataFrame()

    if "geo_usable" in point_df.columns:
        point_df = point_df[point_df["geo_usable"].fillna(False).astype(bool)].copy()

    point_df["latitude"] = pd.to_numeric(point_df["latitude"], errors="coerce")
    point_df["longitude"] = pd.to_numeric(point_df["longitude"], errors="coerce")
    point_df = point_df.dropna(subset=["latitude", "longitude"]).copy()
    return point_df


def lookup_geo_anchor(plan: Optional[Dict[str, object]]) -> Optional[Dict[str, object]]:
    if not isinstance(plan, dict):
        return None

    hints = plan.get("hints", {})
    if not isinstance(hints, dict):
        hints = {}

    geo_anchor_type = str(plan.get("geo_anchor_type") or "").strip().lower()
    coordinates = hints.get("coordinates")
    if isinstance(coordinates, dict):
        try:
            lat = float(coordinates["lat"])
            lon = float(coordinates["lon"])
        except (KeyError, TypeError, ValueError):
            lat = None
            lon = None
        if lat is not None and lon is not None:
            return {
                "type": "point" if geo_anchor_type in {"", "point"} else geo_anchor_type,
                "latitude": lat,
                "longitude": lon,
                "radius_km": float(hints.get("radius_km", 0.0) or 0.0),
            }

    target_county = plan.get("target_county")
    if target_county:
        return {
            "type": "county" if geo_anchor_type in {"", "county"} else geo_anchor_type,
            "target_county": str(target_county),
        }

    return None


def effective_map_county(selected_county: Optional[str], plan: Optional[Dict[str, object]]) -> Optional[str]:
    if selected_county and selected_county != "All Counties":
        return selected_county

    anchor = lookup_geo_anchor(plan)
    if anchor and anchor.get("type") == "county":
        return str(anchor.get("target_county") or "")
    return selected_county


def should_render_map(
    records: List[Dict],
    selected_county: Optional[str],
    route_type: Optional[str],
    plan: Optional[Dict[str, object]] = None,
) -> bool:
    if not map_points_df(records, selected_county).empty:
        return True

    route = str(route_type or "").strip().lower()
    anchor = lookup_geo_anchor(plan)
    return route == "lookup" and anchor is not None


def point_radius_polygon(anchor: Optional[Dict[str, object]], steps: int = 72) -> List[List[float]]:
    if not anchor or anchor.get("type") != "point":
        return []

    radius_km = float(anchor.get("radius_km", 0.0) or 0.0)
    if radius_km <= 0:
        return []

    center = (float(anchor["latitude"]), float(anchor["longitude"]))
    polygon: List[List[float]] = []
    for idx in range(max(12, int(steps))):
        bearing = (360.0 * idx) / max(12, int(steps))
        destination = geopy_distance(kilometers=radius_km).destination(center, bearing)
        polygon.append([float(destination.longitude), float(destination.latitude)])

    if polygon:
        polygon.append(polygon[0])
    return polygon


def map_view_state_config(
    records: List[Dict],
    selected_county: Optional[str],
    plan: Optional[Dict[str, object]] = None,
) -> Dict[str, float]:
    point_df = map_points_df(records, selected_county)
    anchor = lookup_geo_anchor(plan)

    latitudes: List[float] = []
    longitudes: List[float] = []
    if not point_df.empty:
        latitudes.extend(point_df["latitude"].astype(float).tolist())
        longitudes.extend(point_df["longitude"].astype(float).tolist())

    if anchor and anchor.get("type") == "point":
        latitudes.append(float(anchor["latitude"]))
        longitudes.append(float(anchor["longitude"]))

    if not latitudes or not longitudes:
        return dict(DEFAULT_MAP_VIEW)

    min_lat = min(latitudes)
    max_lat = max(latitudes)
    min_lon = min(longitudes)
    max_lon = max(longitudes)
    center_lat = (min_lat + max_lat) / 2.0
    center_lon = (min_lon + max_lon) / 2.0

    lat_span = max(max_lat - min_lat, 0.05)
    lon_span = max(max_lon - min_lon, 0.05)

    if anchor and anchor.get("type") == "point":
        radius_km = float(anchor.get("radius_km", 0.0) or 0.0)
        if radius_km > 0:
            lat_span = max(lat_span, (radius_km / 110.574) * 2.2)
            lon_divisor = 111.320 * max(math.cos(math.radians(center_lat)), 0.25)
            lon_span = max(lon_span, (radius_km / lon_divisor) * 2.2)

    span = max(lat_span, lon_span)
    zoom = _span_to_zoom(span)
    return {
        "latitude": center_lat,
        "longitude": center_lon,
        "zoom": zoom,
        "pitch": 0.0,
    }


def _span_to_zoom(span_degrees: float) -> float:
    if span_degrees >= 25:
        return 4.4
    if span_degrees >= 12:
        return 5.1
    if span_degrees >= 6:
        return 5.9
    if span_degrees >= 3:
        return 6.6
    if span_degrees >= 1.5:
        return 7.2
    if span_degrees >= 0.75:
        return 8.0
    if span_degrees >= 0.35:
        return 8.8
    return 9.6
