from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple


@dataclass
class QueryPlan:
    classification: str
    sql: bool
    geo: bool
    vector: bool
    hints: Dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, object]:
        return {
            "classification": self.classification,
            "sql": self.sql,
            "geo": self.geo,
            "vector": self.vector,
            "hints": self.hints,
        }


class QueryPlanner:
    GEO_KEYWORDS = {"near", "within", "distance", "km", "mile", "miles", "mi", "closest", "coordinate", "coordinates", "radius"}
    SQL_KEYWORDS = {"top", "highest", "most", "employment", "employees", "industry", "list"}
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

    def plan(self, question: str) -> Dict[str, object]:
        text = question.strip()
        lower = text.lower()

        hints: Dict[str, object] = {}
        coords = self._extract_coordinates(lower)
        radius = self._extract_radius_km(lower)
        city = self._extract_city(text)
        metric = self._extract_metric(lower)
        oem = self._extract_oem(lower)
        industry = self._extract_industry(text)
        category_term = self._extract_category_term(lower)
        capability_term = self._extract_capability_term(lower)
        facility_city = self._extract_facility_city(lower)

        if coords:
            hints["coordinates"] = {"lat": coords[0], "lon": coords[1]}
        if radius:
            hints["radius_km"] = radius
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

        geo_signal = bool(coords) or self._contains_keyword(lower, self.GEO_KEYWORDS)
        sql_signal = (
            bool(metric or industry or oem or category_term or capability_term)
            or ("top" in lower and "company" in lower)
            or ("list" in lower and "company" in lower)
        )
        vector_signal = bool(oem or capability_term) or self._contains_keyword(lower, self.VECTOR_KEYWORDS)

        if (city or facility_city) and ("near" in lower or "within" in lower):
            geo_signal = True

        if not any([sql_signal, geo_signal, vector_signal]):
            vector_signal = True

        true_count = sum([sql_signal, geo_signal, vector_signal])
        if true_count > 1:
            classification = "HYBRID_QUERY"
        elif sql_signal:
            classification = "SQL_QUERY"
        elif geo_signal:
            classification = "GEO_QUERY"
        else:
            classification = "VECTOR_QUERY"

        if geo_signal and "radius_km" not in hints:
            hints["radius_km"] = 100.0

        return QueryPlan(
            classification=classification,
            sql=sql_signal,
            geo=geo_signal,
            vector=vector_signal,
            hints=hints,
        ).to_dict()

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
    def _extract_radius_km(text: str) -> Optional[float]:
        match = re.search(r"(\d+(?:\.\d+)?)\s*km\b", text)
        if match:
            return float(match.group(1))
        match = re.search(r"(\d+(?:\.\d+)?)\s*(?:miles?|mi)\b", text)
        if match:
            return float(match.group(1)) * 1.60934
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
                if "km" not in city.lower() and "mile" not in city.lower():
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
