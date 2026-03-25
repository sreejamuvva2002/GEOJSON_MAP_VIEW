from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional

import duckdb
import pandas as pd
import pydeck as pdk
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.geo_utils import canonical_county_display_name, normalize_county_name
from frontend.map_utils import (
    effective_map_county,
    lookup_geo_anchor,
    map_points_df,
    map_view_state_config,
    point_radius_polygon,
    should_render_map,
)

st.set_page_config(page_title="Hybrid Geospatial RAG Chatbot", layout="wide")

BACKEND_CHAT_URL = os.getenv("BACKEND_CHAT_URL", "http://127.0.0.1:8000/chat")
CHAT_TIMEOUT_SECONDS = int(os.getenv("BACKEND_CHAT_TIMEOUT", "120"))
BACKEND_STARTUP_TIMEOUT_SECONDS = int(os.getenv("BACKEND_STARTUP_TIMEOUT", "35"))
AUTO_BACKEND_PORTS = [8000, 8001, 8002]
GEOJSON_PATH = PROJECT_ROOT / "data" / "Counties_Georgia.geojson"
DB_PATH = PROJECT_ROOT / "data" / "gnem.duckdb"
BACKEND_LOG_PATH = PROJECT_ROOT / "data" / "frontend_backend_startup.log"


def health_url_from_chat_url(chat_url: str) -> str:
    if chat_url.endswith("/chat"):
        return chat_url[: -len("/chat")] + "/health"
    return chat_url.rstrip("/") + "/health"


def backend_health_payload(chat_url: str) -> Optional[Dict]:
    health_url = health_url_from_chat_url(chat_url)
    try:
        with urllib.request.urlopen(health_url, timeout=3) as resp:
            if resp.status != 200:
                return None
            return json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None


def backend_is_healthy(chat_url: str) -> bool:
    payload = backend_health_payload(chat_url)
    return bool(payload and payload.get("pipeline_loaded"))


def chat_url_from_port(port: int) -> str:
    return f"http://127.0.0.1:{int(port)}/chat"


def parse_port_from_url(url: str) -> int | None:
    try:
        parsed = urllib.parse.urlparse(url)
        return parsed.port
    except Exception:
        return None


def is_port_listening(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", int(port)), timeout=1):
            return True
    except Exception:
        return False


def _windows_creationflags() -> int:
    flags = 0
    if os.name == "nt":
        detached = getattr(subprocess, "DETACHED_PROCESS", 0)
        new_group = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        flags = detached | new_group
    return flags


def _backend_log_tail(lines: int = 20) -> str:
    if not BACKEND_LOG_PATH.exists():
        return ""
    try:
        content = BACKEND_LOG_PATH.read_text(encoding="utf-8", errors="ignore").splitlines()
        return "\n".join(content[-lines:])
    except Exception:
        return ""


def wait_for_backend(chat_url: str, timeout_seconds: int = BACKEND_STARTUP_TIMEOUT_SECONDS) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if backend_is_healthy(chat_url):
            return True
        time.sleep(1)
    return False


def start_backend_on_port(port: int) -> bool:
    target_url = chat_url_from_port(port)
    if backend_is_healthy(target_url):
        return True

    BACKEND_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with BACKEND_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(f"\n=== backend start attempt on port {port} ===\n")
        try:
            subprocess.Popen(
                ["python", "-m", "uvicorn", "backend.main:app", "--host", "127.0.0.1", "--port", str(int(port))],
                cwd=str(PROJECT_ROOT),
                stdout=handle,
                stderr=handle,
                creationflags=_windows_creationflags(),
            )
        except Exception as exc:
            handle.write(f"Failed to start backend: {exc}\n")
            return False
    return wait_for_backend(target_url, timeout_seconds=BACKEND_STARTUP_TIMEOUT_SECONDS)


def discover_backend_url() -> str:
    preferred = st.session_state.get("backend_url") or BACKEND_CHAT_URL
    candidates = [preferred, BACKEND_CHAT_URL] + [chat_url_from_port(p) for p in AUTO_BACKEND_PORTS]

    deduped = []
    seen = set()
    for url in candidates:
        if url and url not in seen:
            seen.add(url)
            deduped.append(url)

    for url in deduped:
        if backend_is_healthy(url):
            st.session_state["backend_url"] = url
            return url

    ports = []
    preferred_port = parse_port_from_url(preferred)
    if preferred_port:
        ports.append(preferred_port)
    for p in AUTO_BACKEND_PORTS:
        if p not in ports:
            ports.append(p)

    attempts = 0
    for port in ports:
        if attempts >= 2:
            break
        url = chat_url_from_port(port)
        if is_port_listening(port) and not backend_is_healthy(url):
            attempts += 1
            continue
        attempts += 1
        if start_backend_on_port(port):
            st.session_state["backend_url"] = url
            return url

    tail = _backend_log_tail()
    detail = f"\n\nBackend startup log tail:\n{tail}" if tail else ""
    raise urllib.error.URLError(
        "Backend is not reachable. Start it manually with: uvicorn backend.main:app --reload --port 8000" + detail
    )


def call_backend(question: str, selected_county: Optional[str]) -> Dict:
    backend_url = discover_backend_url()
    payload = json.dumps(
        {
            "question": question,
            "selected_county": selected_county,
            "mode": "ui",
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        backend_url,
        data=payload,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=CHAT_TIMEOUT_SECONDS) as resp:
        return json.loads(resp.read().decode("utf-8"))


@st.cache_data(show_spinner=False)
def load_county_geojson() -> Dict:
    if not GEOJSON_PATH.exists():
        return {"type": "FeatureCollection", "features": []}
    payload = json.loads(GEOJSON_PATH.read_text(encoding="utf-8"))
    for feature in payload.get("features", []):
        props = feature.setdefault("properties", {})
        county_name = (
            props.get("county_name")
            or props.get("NAME10")
            or props.get("NAME")
            or str(props.get("NAMELSAD10", "")).replace("County", "").strip()
        )
        props["county_name"] = canonical_county_display_name(county_name) or str(county_name)
        props["county_key"] = normalize_county_name(props["county_name"])
    return payload


@st.cache_data(show_spinner=False)
def load_county_summary() -> pd.DataFrame:
    if not DB_PATH.exists():
        return pd.DataFrame(columns=["county_name", "company_count"])
    try:
        with duckdb.connect(str(DB_PATH), read_only=True) as con:
            return con.execute(
                """
                SELECT county_name, company_count
                FROM county_company_counts
                ORDER BY county_name
                """
            ).fetchdf()
    except Exception:
        return pd.DataFrame(columns=["county_name", "company_count"])


def available_counties() -> List[str]:
    summary = load_county_summary()
    if summary.empty:
        geojson = load_county_geojson()
        counties = sorted(
            {
                canonical_county_display_name(feature.get("properties", {}).get("county_name"))
                for feature in geojson.get("features", [])
            }
        )
        return [county for county in counties if county]
    return summary["county_name"].astype(str).tolist()


def render_sources(sources: List[str]) -> None:
    if not sources:
        return
    st.markdown("**Evidence**")
    for source in sources:
        st.markdown(f"- {source}")


def render_chunks(chunks: List[Dict]) -> None:
    if not chunks:
        return
    st.markdown("**Retrieved Evidence (Detailed)**")
    df = pd.DataFrame(chunks)
    preferred_cols = ["evidence_id", "engine", "company", "chunk_type", "score", "text"]
    ordered = [c for c in preferred_cols if c in df.columns] + [c for c in df.columns if c not in preferred_cols]
    st.dataframe(df[ordered], use_container_width=True)


def render_table(records: List[Dict], title: str = "**Retrieved Results**") -> None:
    if not records:
        return
    st.markdown(title)
    df = pd.DataFrame(records)
    preferred_cols = [
        "company",
        "county_name",
        "industry_group",
        "city",
        "county",
        "primary_oems",
        "metric_value",
        "map_weight",
        "employment",
        "distance_miles",
        "distance_km",
        "distance_reference",
        "filter_distance_miles",
        "filter_distance_km",
        "filter_distance_reference",
        "distance_to_boundary_miles",
        "distance_to_boundary_km",
        "coordinate_source",
        "score",
    ]
    ordered = [c for c in preferred_cols if c in df.columns] + [c for c in df.columns if c not in preferred_cols]
    st.dataframe(df[ordered], use_container_width=True)


def render_map(records: List[Dict], selected_county: Optional[str], plan: Optional[Dict] = None) -> None:
    map_selected_county = effective_map_county(selected_county, plan)
    point_seed_df = map_points_df(records, map_selected_county)
    anchor = lookup_geo_anchor(plan)
    if point_seed_df.empty and anchor is None:
        return

    geojson = load_county_geojson()
    summary_df = load_county_summary()
    features = geojson.get("features", [])
    selected_key = normalize_county_name(map_selected_county) if map_selected_county and map_selected_county != "All Counties" else None
    plan_hints = dict(plan.get("hints", {})) if isinstance(plan, dict) and isinstance(plan.get("hints", {}), dict) else {}

    record_df = pd.DataFrame(records).copy()
    filtered_counts = {}
    role_summary = {}
    category_summary = {}
    if not record_df.empty and "county" in record_df.columns:
        record_df["county_key"] = record_df["county"].apply(normalize_county_name)
        filtered_counts = record_df["county_key"].value_counts().to_dict()
        if "ev_supply_chain_role" in record_df.columns:
            for county_key, group in record_df.groupby("county_key"):
                top_roles = group["ev_supply_chain_role"].fillna("Unknown").value_counts().head(3)
                role_summary[county_key] = ", ".join(f"{idx} ({val})" for idx, val in top_roles.items())
        if "category" in record_df.columns:
            for county_key, group in record_df.groupby("county_key"):
                top_categories = group["category"].fillna("Unknown").value_counts().head(3)
                category_summary[county_key] = ", ".join(f"{idx} ({val})" for idx, val in top_categories.items())

    total_lookup = {}
    if not summary_df.empty:
        total_lookup = {
            normalize_county_name(row["county_name"]): int(row["company_count"])
            for _, row in summary_df.iterrows()
        }

    for feature in features:
        props = feature.setdefault("properties", {})
        county_key = props.get("county_key")
        total_count = total_lookup.get(county_key, 0)
        filtered_count = int(filtered_counts.get(county_key, 0))
        props["total_count"] = total_count
        props["filtered_count"] = filtered_count
        props["role_summary"] = role_summary.get(county_key, "No retrieved roles")
        props["category_summary"] = category_summary.get(county_key, "No retrieved categories")
        if selected_key and county_key == selected_key:
            props["fill_color"] = [237, 167, 52, 170]
        elif filtered_count > 0:
            intensity = min(220, 70 + filtered_count * 18)
            props["fill_color"] = [49, 111, 97, intensity]
        else:
            props["fill_color"] = [203, 214, 221, 70]

        # Mirror tooltip fields onto the top-level feature object so pydeck
        # can interpolate them consistently for GeoJSON hover tooltips.
        feature["county_name"] = props["county_name"]
        feature["total_count"] = total_count
        feature["filtered_count"] = filtered_count
        feature["role_summary"] = props["role_summary"]
        feature["category_summary"] = props["category_summary"]

    county_layer = pdk.Layer(
        "GeoJsonLayer",
        data=geojson,
        pickable=True,
        stroked=True,
        filled=True,
        extruded=False,
        opacity=0.45,
        get_fill_color="properties.fill_color",
        get_line_color=[52, 73, 94, 160],
        line_width_min_pixels=1,
    )

    layers = [county_layer]
    if anchor and anchor.get("type") == "point":
        polygon = point_radius_polygon(anchor)
        if polygon:
            radius_df = pd.DataFrame([{"polygon": polygon}])
            radius_layer = pdk.Layer(
                "PolygonLayer",
                data=radius_df,
                get_polygon="polygon",
                get_fill_color=[237, 167, 52, 26],
                get_line_color=[237, 167, 52, 190],
                line_width_min_pixels=2,
                stroked=True,
                filled=True,
                pickable=False,
            )
            layers.append(radius_layer)

    point_df = point_seed_df.copy()
    if not point_df.empty:
        if "county_key" not in point_df.columns and "county" in point_df.columns:
            point_df["county_key"] = point_df["county"].apply(normalize_county_name)
        point_df["county_name"] = point_df.get("county", pd.Series(index=point_df.index)).fillna("Unknown county")
        point_df["total_count"] = point_df["county_key"].map(total_lookup).fillna(0).astype(int)
        point_df["filtered_count"] = point_df["county_key"].map(filtered_counts).fillna(0).astype(int)
        point_df["role_summary"] = point_df["county_key"].map(role_summary).fillna("No retrieved roles")
        point_df["category_summary"] = point_df["county_key"].map(category_summary).fillna("No retrieved categories")
        point_df["map_weight"] = pd.to_numeric(point_df.get("map_weight"), errors="coerce").fillna(0.5).clip(lower=0.05, upper=1.0)
        point_df["radius"] = point_df["map_weight"].apply(lambda v: 2500.0 + float(v) * 18000.0)
        point_df["fill_color"] = point_df["map_weight"].apply(lambda v: [18, 75, 120, int(140 + min(100, v * 70))])
        point_df["tooltip_company"] = point_df.get("company", pd.Series(index=point_df.index)).fillna("Unknown company")
        point_df["tooltip_role"] = point_df.get("ev_supply_chain_role", pd.Series(index=point_df.index)).fillna("Unknown role")
        point_df["tooltip_category"] = point_df.get("category", pd.Series(index=point_df.index)).fillna("Unknown category")
        point_df["tooltip_county"] = point_df.get("county", pd.Series(index=point_df.index)).fillna("Unknown county")
        point_df["tooltip_lat"] = point_df["latitude"].apply(lambda v: f"{float(v):.5f}")
        point_df["tooltip_lon"] = point_df["longitude"].apply(lambda v: f"{float(v):.5f}")
        point_layer = pdk.Layer(
            "ScatterplotLayer",
            data=point_df,
            get_position="[longitude, latitude]",
            get_fill_color="fill_color",
            get_line_color=[21, 31, 43, 180],
            line_width_min_pixels=1,
            stroked=True,
            filled=True,
            pickable=True,
            get_radius="radius",
        )
        layers.append(point_layer)

    if anchor and anchor.get("type") == "point":
        radius_km = float(anchor.get("radius_km", 0.0) or 0.0)
        anchor_df = pd.DataFrame(
            [
                {
                    "latitude": float(anchor["latitude"]),
                    "longitude": float(anchor["longitude"]),
                    "county_name": "Search area",
                    "total_count": int(len(point_seed_df)),
                    "filtered_count": int(len(point_seed_df)),
                    "role_summary": f"Radius {radius_km:.1f} km",
                    "category_summary": "Point-radius lookup",
                    "tooltip_company": "Search center",
                    "tooltip_role": f"Radius: {radius_km:.1f} km",
                    "tooltip_category": str(plan_hints.get("capability_term") or "Requested search"),
                    "tooltip_county": "Query coordinates",
                    "tooltip_lat": f"{float(anchor['latitude']):.5f}",
                    "tooltip_lon": f"{float(anchor['longitude']):.5f}",
                    "fill_color": [237, 167, 52, 235],
                    "radius": 9500.0,
                }
            ]
        )
        anchor_layer = pdk.Layer(
            "ScatterplotLayer",
            data=anchor_df,
            get_position="[longitude, latitude]",
            get_fill_color="fill_color",
            get_line_color=[102, 58, 0, 220],
            line_width_min_pixels=2,
            stroked=True,
            filled=True,
            pickable=True,
            get_radius="radius",
        )
        layers.append(anchor_layer)

    view_state = pdk.ViewState(**map_view_state_config(records, map_selected_county, plan))
    tooltip = {
        "html": (
            "<b>{county_name} County</b><br/>"
            "Total geo-usable companies: {total_count}<br/>"
            "Retrieved companies: {filtered_count}<br/>"
            "Roles: {role_summary}<br/>"
            "Categories: {category_summary}<br/><br/>"
            "<b>{tooltip_company}</b><br/>"
            "Role: {tooltip_role}<br/>"
            "Category: {tooltip_category}<br/>"
            "County: {tooltip_county}<br/>"
            "Latitude: {tooltip_lat}<br/>"
            "Longitude: {tooltip_lon}"
        ),
        "style": {"backgroundColor": "#13202b", "color": "white"},
    }

    st.markdown("**Georgia County Map**")
    if anchor and anchor.get("type") == "point":
        st.caption(
            "County polygons are authoritative boundaries. The orange marker and ring show the requested point-radius search; "
            "company distances use geodesic distance."
        )
    else:
        st.caption(
            "County polygons are authoritative boundaries. Point-radius uses geodesic distance; "
            "county distance uses polygon distance in EPSG:5070."
        )
    st.pydeck_chart(
        pdk.Deck(
            map_style="light_no_labels",
            initial_view_state=view_state,
            tooltip=tooltip,
            layers=layers,
        ),
        use_container_width=True,
    )


def render_assistant_message(msg: Dict) -> None:
    st.markdown(msg["content"])
    st.caption(
        f"Model: {msg.get('model_used', 'unknown')} | Route: {msg.get('route_type', 'unknown')} | Mode: {msg.get('mode', 'unknown')}"
    )


def render_assistant_map_preview(msg: Dict, include_map: bool = True) -> None:
    if not include_map:
        return

    plan = msg.get("plan") if isinstance(msg.get("plan"), dict) else None
    message_selected_county = effective_map_county(msg.get("selected_county"), plan)
    if should_render_map(msg.get("retrieved_companies", []), message_selected_county, msg.get("route_type"), plan):
        render_map(msg.get("retrieved_companies", []), message_selected_county, plan)


def render_assistant_details(msg: Dict, include_map: bool = True) -> None:
    render_sources(msg.get("sources", []))
    render_chunks(msg.get("retrieved_chunks", []))

    if include_map:
        render_assistant_map_preview(msg, include_map=True)

    render_table(msg.get("retrieved_companies", []))


st.title("Hybrid Geospatial RAG Chatbot")
st.caption("Ask questions over GNEM company data using deterministic geo/analytics plus document retrieval.")

county_options = ["All Counties"] + available_counties()

with st.sidebar:
    st.markdown("### Backend")
    effective_url = st.session_state.get("backend_url", BACKEND_CHAT_URL)
    st.code(effective_url, language="text")
    health = backend_health_payload(effective_url)
    backend_ok = bool(health and health.get("pipeline_loaded"))
    st.markdown(f"Status: {'Healthy' if backend_ok else 'Not reachable'}")
    if health and health.get("error"):
        st.caption(f"Backend detail: {health['error']}")
    if not backend_ok:
        st.caption("UI will auto-attempt to start backend on first chat request.")
        if st.button("Start Backend Now"):
            try:
                resolved = discover_backend_url()
                st.session_state["backend_url"] = resolved
                st.success(f"Backend ready at {resolved}")
            except Exception as exc:
                st.error(str(exc))
                tail = _backend_log_tail()
                if tail:
                    st.code(tail, language="text")

    st.markdown("### County Focus")
    selected_county = st.selectbox("Highlight County", options=county_options, index=0)
    st.session_state["selected_county"] = selected_county

    st.markdown("### Example Questions")
    st.markdown("- Which companies are in Troup County?")
    st.markdown("- Which suppliers are within 25 miles of Troup County?")
    st.markdown("- List battery companies within 100 km of 33.7490, -84.3880.")
    st.markdown("- Counties with 0 Tier 1")
    st.markdown("- Top companies by employment.")

if "messages" not in st.session_state:
    st.session_state.messages = []

assistant_positions = [idx for idx, message in enumerate(st.session_state.messages) if message.get("role") == "assistant"]
latest_assistant_position = assistant_positions[-1] if assistant_positions else None
assistant_counter = 0
for idx, msg in enumerate(st.session_state.messages):
    with st.chat_message(msg["role"]):
        if msg["role"] == "assistant":
            render_assistant_message(msg)
            render_assistant_map_preview(msg, include_map=(idx == latest_assistant_position))
        else:
            st.markdown(msg["content"])
    if msg["role"] == "assistant":
        assistant_counter += 1
        with st.expander(f"Details for response {assistant_counter}", expanded=False):
            render_assistant_details(msg, include_map=False)

user_question = st.chat_input("Ask a question about GNEM companies and geospatial relationships...")

if user_question:
    st.session_state.messages.append({"role": "user", "content": user_question})
    with st.chat_message("user"):
        st.markdown(user_question)

    try:
        with st.spinner("Running deterministic geo/analytic routing plus evidence retrieval..."):
            message_selected_county = None if st.session_state.get("selected_county") == "All Counties" else st.session_state.get("selected_county")
            result = call_backend(user_question, message_selected_county)

        assistant_message = {
            "role": "assistant",
            "content": result.get("answer", "No answer returned."),
            "sources": result.get("sources", []),
            "retrieved_chunks": result.get("retrieved_chunks", []),
            "retrieved_companies": result.get("retrieved_companies", []),
            "plan": result.get("plan", {}),
            "route_type": result.get("route_type", "unknown"),
            "mode": result.get("mode", "unknown"),
            "model_used": result.get("model_used", "unknown"),
            "selected_county": message_selected_county,
        }
        st.session_state.messages.append(assistant_message)
        with st.chat_message("assistant"):
            render_assistant_message(assistant_message)
            render_assistant_map_preview(assistant_message, include_map=True)
        with st.expander(f"Details for response {assistant_counter + 1}", expanded=True):
            render_assistant_details(assistant_message, include_map=False)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        st.error(f"Backend HTTP error: {exc.code} - {detail}")
        tail = _backend_log_tail()
        if tail:
            st.code(tail, language="text")
    except urllib.error.URLError as exc:
        st.error(f"Backend connection failed: {exc.reason}")
        tail = _backend_log_tail()
        if tail:
            st.code(tail, language="text")
    except Exception as exc:
        st.error(f"Unexpected error: {exc}")
        tail = _backend_log_tail()
        if tail:
            st.code(tail, language="text")
