from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from pyproj import Transformer
from shapely.geometry import Point, mapping, shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform, unary_union
from shapely.validation import make_valid

PROJECTED_CRS = "EPSG:5070"
WGS84_CRS = "EPSG:4326"
METERS_TO_MILES = 0.000621371

_COUNTY_SUFFIX_RE = re.compile(r"\bcounty\b", flags=re.IGNORECASE)
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_TO_PROJECTED = Transformer.from_crs(WGS84_CRS, PROJECTED_CRS, always_xy=True)


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    text = str(value).strip()
    return not text or text.lower() in {"nan", "none", "null"}


def normalize_county_name(value: object) -> Optional[str]:
    if _is_missing(value):
        return None
    text = _COUNTY_SUFFIX_RE.sub("", str(value)).strip().lower()
    text = _NON_ALNUM_RE.sub(" ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def canonical_county_display_name(value: object) -> Optional[str]:
    normalized = normalize_county_name(value)
    if not normalized:
        return None
    return " ".join(part.capitalize() for part in normalized.split())


def stable_company_slug(company_name: object, fallback: str = "company") -> str:
    if _is_missing(company_name):
        return fallback
    slug = re.sub(r"[^a-z0-9]+", "-", str(company_name).strip().lower()).strip("-")
    return slug or fallback


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def file_sha256(path: Path) -> Optional[str]:
    if not Path(path).exists():
        return None
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _polygonalize(geometry: BaseGeometry) -> BaseGeometry:
    if geometry.geom_type in {"Polygon", "MultiPolygon"}:
        return geometry
    polygon_parts = []
    if hasattr(geometry, "geoms"):
        polygon_parts = [geom for geom in geometry.geoms if geom.geom_type in {"Polygon", "MultiPolygon"}]
    if not polygon_parts:
        return geometry
    if len(polygon_parts) == 1:
        return polygon_parts[0]
    return unary_union(polygon_parts)


def _repair_geometry(geometry: BaseGeometry) -> tuple[BaseGeometry, bool, str]:
    repaired = geometry
    repaired_flag = False
    repair_steps: List[str] = []

    if not repaired.is_valid:
        repaired = _polygonalize(make_valid(repaired))
        repaired_flag = True
        repair_steps.append("make_valid")

    if not repaired.is_valid:
        repaired = _polygonalize(repaired.buffer(0))
        repaired_flag = True
        repair_steps.append("buffer0")

    repaired = _polygonalize(repaired)
    if repaired.is_empty:
        raise ValueError("County geometry became empty during repair.")

    method = "+".join(repair_steps) if repair_steps else "none"
    return repaired, repaired_flag, method


@dataclass
class CountyGeometry:
    county_id: str
    county_name: str
    county_key: str
    county_fips: Optional[str]
    geoid: Optional[str]
    geometry: BaseGeometry
    projected_geometry: BaseGeometry
    centroid_latitude: float
    centroid_longitude: float
    repair_applied: bool
    repair_method: str
    properties: Dict[str, Any]


@dataclass
class CountyGeometryIndex:
    counties: List[CountyGeometry]
    geometry_hash: str
    repaired_counties: List[str]
    repaired_feature_collection: Dict[str, Any]

    @property
    def repair_count(self) -> int:
        return len(self.repaired_counties)

    @property
    def by_county_id(self) -> Dict[str, CountyGeometry]:
        return {county.county_id: county for county in self.counties}

    @property
    def by_county_key(self) -> Dict[str, CountyGeometry]:
        return {county.county_key: county for county in self.counties}

    @property
    def county_options(self) -> List[str]:
        return sorted(county.county_name for county in self.counties)

    @property
    def centroid_lookup(self) -> Dict[str, tuple[float, float]]:
        return {
            county.county_key: (county.centroid_latitude, county.centroid_longitude)
            for county in self.counties
        }


def project_geometry(geometry: BaseGeometry) -> BaseGeometry:
    return transform(_TO_PROJECTED.transform, geometry)


def load_county_geometries(geojson_path: Path) -> CountyGeometryIndex:
    payload = json.loads(Path(geojson_path).read_text(encoding="utf-8"))

    counties: List[CountyGeometry] = []
    repaired_features: List[Dict[str, Any]] = []
    repaired_counties: List[str] = []

    for feature in payload.get("features", []):
        properties = dict(feature.get("properties", {}))
        county_name = (
            properties.get("NAME10")
            or properties.get("NAME")
            or str(properties.get("NAMELSAD10", "")).replace("County", "").strip()
        )
        county_key = normalize_county_name(county_name)
        if not county_key:
            continue

        county_fips = str(properties.get("COUNTYFP10") or "").strip() or None
        geoid = str(properties.get("GEOID10") or "").strip() or None
        county_id = county_fips or county_key

        raw_geometry = shape(feature["geometry"])
        repaired_geometry, repair_applied, repair_method = _repair_geometry(raw_geometry)
        if repair_applied:
            repaired_counties.append(canonical_county_display_name(county_name) or county_key)

        centroid = repaired_geometry.centroid
        projected_geometry = project_geometry(repaired_geometry)

        county_record = CountyGeometry(
            county_id=county_id,
            county_name=canonical_county_display_name(county_name) or county_key.title(),
            county_key=county_key,
            county_fips=county_fips,
            geoid=geoid,
            geometry=repaired_geometry,
            projected_geometry=projected_geometry,
            centroid_latitude=float(centroid.y),
            centroid_longitude=float(centroid.x),
            repair_applied=repair_applied,
            repair_method=repair_method,
            properties=properties,
        )
        counties.append(county_record)
        repaired_features.append(
            {
                "type": "Feature",
                "properties": {
                    **properties,
                    "county_id": county_record.county_id,
                    "county_name": county_record.county_name,
                    "county_key": county_record.county_key,
                    "repair_applied": county_record.repair_applied,
                    "repair_method": county_record.repair_method,
                },
                "geometry": mapping(repaired_geometry),
            }
        )

    repaired_feature_collection = {
        "type": "FeatureCollection",
        "features": sorted(
            repaired_features,
            key=lambda item: (
                str(item["properties"].get("county_id") or ""),
                str(item["properties"].get("county_name") or ""),
            ),
        ),
    }
    geometry_hash = sha256_text(json.dumps(repaired_feature_collection, sort_keys=True))
    counties = sorted(counties, key=lambda county: (county.county_name, county.county_id))

    return CountyGeometryIndex(
        counties=counties,
        geometry_hash=geometry_hash,
        repaired_counties=sorted(set(repaired_counties)),
        repaired_feature_collection=repaired_feature_collection,
    )


def resolve_county_geometry(county_index: CountyGeometryIndex, county_name: object) -> Optional[CountyGeometry]:
    county_key = normalize_county_name(county_name)
    if not county_key:
        return None
    return county_index.by_county_key.get(county_key)


def compute_county_for_point(
    county_index: CountyGeometryIndex,
    latitude: float,
    longitude: float,
) -> Optional[CountyGeometry]:
    point = Point(float(longitude), float(latitude))
    for county in county_index.counties:
        if county.geometry.contains(point) or county.geometry.touches(point):
            return county
    return None


def compute_point_to_county_distance_miles(
    county_index: CountyGeometryIndex,
    latitude: float,
    longitude: float,
    county_name: object,
) -> Optional[float]:
    county = resolve_county_geometry(county_index, county_name)
    if county is None:
        return None

    point = Point(float(longitude), float(latitude))
    if county.geometry.contains(point) or county.geometry.touches(point):
        return 0.0

    projected_point = project_geometry(point)
    distance_meters = float(projected_point.distance(county.projected_geometry))
    return distance_meters * METERS_TO_MILES
