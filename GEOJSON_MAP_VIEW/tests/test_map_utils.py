import pytest

from frontend.map_utils import (
    effective_map_county,
    lookup_geo_anchor,
    map_view_state_config,
    point_radius_polygon,
    should_render_map,
)


def test_point_lookup_anchor_renders_map_without_company_rows() -> None:
    plan = {
        "route_type": "lookup",
        "geo_anchor_type": "point",
        "hints": {
            "coordinates": {"lat": 33.7490, "lon": -84.3880},
            "radius_km": 100.0,
        },
    }

    anchor = lookup_geo_anchor(plan)
    assert anchor == {
        "type": "point",
        "latitude": 33.749,
        "longitude": -84.388,
        "radius_km": 100.0,
    }
    assert should_render_map([], None, "lookup", plan) is True

    polygon = point_radius_polygon(anchor)
    assert len(polygon) == 73
    assert polygon[0] == polygon[-1]

    view_state = map_view_state_config([], None, plan)
    assert view_state["latitude"] == pytest.approx(33.7490)
    assert view_state["longitude"] == pytest.approx(-84.3880)
    assert view_state["zoom"] > 0


def test_effective_map_county_falls_back_to_lookup_target() -> None:
    plan = {
        "route_type": "lookup",
        "geo_anchor_type": "county",
        "target_county": "Troup",
        "hints": {},
    }

    assert effective_map_county(None, plan) == "Troup"
    assert should_render_map([], None, "lookup", plan) is True


def test_mappable_records_render_even_without_lookup_route() -> None:
    records = [
        {
            "company": "Battery Co",
            "county": "Fulton",
            "latitude": 33.76,
            "longitude": -84.39,
            "geo_usable": True,
        }
    ]

    assert should_render_map(records, None, "llm_synthesis", None) is True
