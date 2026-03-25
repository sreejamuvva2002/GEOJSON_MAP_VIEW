from backend.query_planner import QueryPlanner


def test_planner_parses_county_radius_query() -> None:
    planner = QueryPlanner()
    plan = planner.plan("Which suppliers are within 25 miles of Troup County?")

    assert plan["route_type"] == "lookup"
    assert plan["geo"] is True
    assert plan["target_county"] == "Troup"
    assert plan["radius_miles"] == 25.0
    assert plan["geo_anchor_type"] == "county"
    assert plan["requires_polygon_distance"] is True
