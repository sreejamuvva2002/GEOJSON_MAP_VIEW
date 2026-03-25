from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

from backend.geo_utils import canonical_county_display_name, normalize_county_name


@dataclass
class QueryPlan:
    classification: str
    route_type: str
    sql: bool
    geo: bool
    vector: bool
    target_county: Optional[str] = None
    radius_miles: Optional[float] = None
    geo_anchor_type: Optional[str] = None
    analytic_metric: Optional[str] = None
    analytic_term: Optional[str] = None
    requires_polygon_distance: bool = False
    hints: Dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, object]:
        return {
            "classification": self.classification,
            "route_type": self.route_type,
            "sql": self.sql,
            "geo": self.geo,
            "vector": self.vector,
            "target_county": self.target_county,
            "radius_miles": self.radius_miles,
            "geo_anchor_type": self.geo_anchor_type,
            "analytic_metric": self.analytic_metric,
            "analytic_term": self.analytic_term,
            "requires_polygon_distance": self.requires_polygon_distance,
            "hints": self.hints,
        }


class QueryPlanner:
    GEO_KEYWORDS = {"near", "within", "distance", "km", "mile", "miles", "mi", "closest", "coordinate", "coordinates", "radius"}
    SQL_KEYWORDS = {"top", "highest", "most", "employment", "employees", "industry", "list", "counties"}
    VECTOR_KEYWORDS = {"supplier", "suppliers", "battery", "oem", "relationship", "supply", "products"}
    FACILITY_CITY_ALIASES = {
        "kia west point": "West Point",
        "west point facility": "West Point",
        "port of savannah": "Savannah",
        "savannah port": "Savannah",
    }

    OEM_NAMES = [
        "ford",
        "gm",
        "general motors",
        "tesla",
        "rivian",
        "hyundai",
        "kia",
        "toyota",
        "honda",
        "nissan",
        "bmw",
        "mercedes",
        "stellantis",
        "volkswagen",
        "vw",
    ]

    def plan(self, question: str, selected_county: Optional[str] = None) -> Dict[str, object]:
        text = question.strip()
        lower = text.lower()

        hints: Dict[str, object] = {}
        coords = self._extract_coordinates(lower)
        radius_miles = self._extract_radius_miles(lower)
        city = self._extract_city(text)
        metric = self._extract_metric(lower)
        oem = self._extract_oem(lower)
        industry = self._extract_industry(text)
        category_term = self._extract_category_term(lower)
        capability_term = self._extract_capability_term(lower)
        facility_city = self._extract_facility_city(lower)
        county = self._extract_county(text)
        gap_term = self._extract_gap_query(text)

        if coords:
            hints["coordinates"] = {"lat": coords[0], "lon": coords[1]}
        if radius_miles is not None:
            hints["radius_miles"] = radius_miles
            hints["radius_km"] = radius_miles * 1.60934
        if city:
            hints["city"] = city
        if metric:
            hints["metric"] = metric
        if oem:
            hints["oem"] = oem
        if industry:
            hints["industry_group"] = industry
        if category_term:
            hints["category_term"] = category_term
        if capability_term:
            hints["capability_term"] = capability_term
        if facility_city:
            hints["city"] = facility_city
        if county:
            hints["target_county"] = county
        if selected_county:
            hints["selected_county"] = canonical_county_display_name(selected_county)
        if gap_term:
            hints["analytic_term"] = gap_term

        route_type = "llm_synthesis"
        geo_anchor_type = None
        analytic_metric = None
        analytic_term = gap_term
        requires_polygon_distance = False
        sql_signal = False
        geo_signal = False
        vector_signal = False

        if gap_term:
            route_type = "analytic_local"
            analytic_metric = "zero_gap"
            sql_signal = True
        elif county and radius_miles is not None and re.search(r"\bwithin\s+\d+(?:\.\d+)?\s*(?:miles?|mi|km)\s+of\s+.+county\b", lower):
            route_type = "lookup"
            geo_signal = True
            sql_signal = bool(oem or category_term or capability_term)
            geo_anchor_type = "county"
            requires_polygon_distance = True
        elif county and re.search(r"\bin\s+.+county\b", lower):
            route_type = "lookup"
            geo_signal = True
            sql_signal = bool(oem or category_term or capability_term)
            geo_anchor_type = "county"
        elif coords and radius_miles is not None:
            route_type = "lookup"
            geo_signal = True
            sql_signal = bool(oem or category_term or capability_term)
            geo_anchor_type = "point"
        elif (city or facility_city) and ("near" in lower or "within" in lower):
            route_type = "lookup"
            geo_signal = True
            sql_signal = bool(oem or category_term or capability_term)
            geo_anchor_type = "city"
        elif metric:
            route_type = "analytic_local"
            analytic_metric = metric
            sql_signal = True
        else:
            sql_signal = (
                bool(metric or industry or oem or category_term or capability_term)
                or ("top" in lower and "company" in lower)
                or ("list" in lower and "company" in lower)
            )
            vector_signal = bool(oem or capability_term) or self._contains_keyword(lower, self.VECTOR_KEYWORDS)
            if not any([sql_signal, geo_signal, vector_signal]):
                route_type = "web_needed"
            else:
                route_type = "llm_synthesis"

        if route_type == "llm_synthesis" and not vector_signal:
            vector_signal = True
        if geo_signal and "radius_km" not in hints and geo_anchor_type in {"city", "point"}:
            hints["radius_miles"] = 62.1371
            hints["radius_km"] = 100.0
            radius_miles = 62.1371

        classification = self._classify(sql_signal=sql_signal, geo_signal=geo_signal, vector_signal=vector_signal, route_type=route_type)

        return QueryPlan(
            classification=classification,
            route_type=route_type,
            sql=sql_signal,
            geo=geo_signal,
            vector=vector_signal,
            target_county=county,
            radius_miles=radius_miles,
            geo_anchor_type=geo_anchor_type,
            analytic_metric=analytic_metric,
            analytic_term=analytic_term,
            requires_polygon_distance=requires_polygon_distance,
            hints=hints,
        ).to_dict()

    @staticmethod
    def _classify(sql_signal: bool, geo_signal: bool, vector_signal: bool, route_type: str) -> str:
        if route_type == "analytic_local":
            return "ANALYTIC_QUERY"
        true_count = sum([sql_signal, geo_signal, vector_signal])
        if true_count > 1:
            return "HYBRID_QUERY"
        if geo_signal:
            return "GEO_QUERY"
        if sql_signal:
            return "SQL_QUERY"
        if route_type == "web_needed":
            return "WEB_QUERY"
        return "VECTOR_QUERY"

    @staticmethod
    def _contains_keyword(text: str, keywords: set[str]) -> bool:
        return any(word in text for word in keywords)

    @staticmethod
    def _extract_coordinates(text: str) -> Optional[Tuple[float, float]]:
        pattern = re.compile(r"(-?\d{1,2}(?:\.\d+)?)\s*[, ]\s*(-?\d{1,3}(?:\.\d+)?)")
        for match in pattern.finditer(text):
            lat = float(match.group(1))
            lon = float(match.group(2))
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                return lat, lon
        return None

    @staticmethod
    def _extract_radius_miles(text: str) -> Optional[float]:
        match = re.search(r"(\d+(?:\.\d+)?)\s*km\b", text)
        if match:
            return float(match.group(1)) * 0.621371
        match = re.search(r"(\d+(?:\.\d+)?)\s*(?:miles?|mi)\b", text)
        if match:
            return float(match.group(1))
        return None

    @staticmethod
    def _extract_city(text: str) -> Optional[str]:
        patterns = [
            r"\bnear\s+([A-Za-z][A-Za-z\s\-']+?)(?:[?.!,]|$)",
            r"\baround\s+([A-Za-z][A-Za-z\s\-']+?)(?:[?.!,]|$)",
            r"\bclosest\s+to\s+([A-Za-z][A-Za-z\s\-']+?)(?:[?.!,]|$)",
            r"\bwithin\s+\d+(?:\.\d+)?\s*(?:km|miles?|mi)\s+of\s+([A-Za-z][A-Za-z\s\-']+?)(?:[?.!,]|$)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                city = match.group(1).strip()
                if "county" not in city.lower() and "km" not in city.lower() and "mile" not in city.lower():
                    return city.title()
        return None

    @staticmethod
    def _extract_metric(text: str) -> Optional[str]:
        if "employment" in text or "employees" in text:
            return "employment"
        return None

    @staticmethod
    def _extract_category_term(text: str) -> Optional[str]:
        if "tier 2/3" in text:
            return "Tier 2/3"
        if "tier 1/2" in text:
            return "Tier 1/2"
        if "tier 2" in text:
            return "Tier 2"
        if "tier 1" in text:
            return "Tier 1"
        if "oem footprint" in text:
            return "OEM Footprint"
        return None

    @staticmethod
    def _extract_capability_term(text: str) -> Optional[str]:
        for term in ["stamping", "battery", "seating", "electronics", "thermal", "wiring"]:
            if term in text:
                return term
        return None

    def _extract_oem(self, text: str) -> Optional[str]:
        for oem in self.OEM_NAMES:
            if oem in text:
                return oem.title()
        return None

    def _extract_facility_city(self, text: str) -> Optional[str]:
        for phrase, city in self.FACILITY_CITY_ALIASES.items():
            if phrase in text:
                return city
        return None

    @staticmethod
    def _extract_industry(text: str) -> Optional[str]:
        match = re.search(r"industry(?:\s+group)?\s+(?:is\s+|=|:)?([A-Za-z0-9\s/&-]+)", text, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip().title()
        return None

    @staticmethod
    def _extract_county(text: str) -> Optional[str]:
        patterns = [
            r"\bin\s+([A-Za-z][A-Za-z\s\-'.&]+?)\s+county\b",
            r"\bof\s+([A-Za-z][A-Za-z\s\-'.&]+?)\s+county\b",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                return canonical_county_display_name(match.group(1))

        fallback = re.search(r"\b([A-Za-z][A-Za-z\s\-'.&]+?)\s+county\b", text, flags=re.IGNORECASE)
        if fallback:
            county_name = fallback.group(1)
            normalized = normalize_county_name(county_name)
            if normalized:
                return canonical_county_display_name(normalized)
        return None

    @staticmethod
    def _extract_gap_query(text: str) -> Optional[str]:
        match = re.search(r"\bcount(?:y|ies)\s+with\s+0\s+([A-Za-z0-9\s/&-]+?)(?:[?.!,]|$)", text, flags=re.IGNORECASE)
        if not match:
            return None
        return match.group(1).strip().lower()
