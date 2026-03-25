from __future__ import annotations

import json
import os
import socket
import subprocess
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

from backend.geo_utils import canonical_county_display_name, normalize_county_name

st.set_page_config(page_title="Hybrid Geospatial RAG Chatbot", layout="wide")

BACKEND_CHAT_URL = os.getenv("BACKEND_CHAT_URL", "http://127.0.0.1:8000/chat")
PROJECT_ROOT = Path(__file__).resolve().parents[1]
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
        "coordinate_source",
        "score",
    ]
    ordered = [c for c in preferred_cols if c in df.columns] + [c for c in df.columns if c not in preferred_cols]
    st.dataframe(df[ordered], use_container_width=True)


def _filtered_records(records: List[Dict], selected_county: Optional[str]) -> pd.DataFrame:
    df = pd.DataFrame(records).copy()
    if df.empty or not selected_county or selected_county == "All Counties":
        return df
    county_key = normalize_county_name(selected_county)
    if "county_key" in df.columns:
        return df[df["county_key"].fillna("").astype(str) == county_key].copy()
    if "county" in df.columns:
        return df[df["county"].fillna("").apply(normalize_county_name) == county_key].copy()
    return df


def render_map(records: List[Dict], selected_county: Optional[str]) -> None:
    geojson = load_county_geojson()
    summary_df = load_county_summary()
    features = geojson.get("features", [])
    selected_key = normalize_county_name(selected_county) if selected_county and selected_county != "All Counties" else None

    record_df = pd.DataFrame(records).copy()
    filtered_df = _filtered_records(records, selected_county)
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
    point_df = filtered_df.copy() if not filtered_df.empty else pd.DataFrame(records).copy()
    if not point_df.empty and {"latitude", "longitude"}.issubset(point_df.columns):
        point_df["latitude"] = pd.to_numeric(point_df["latitude"], errors="coerce")
        point_df["longitude"] = pd.to_numeric(point_df["longitude"], errors="coerce")
        point_df = point_df.dropna(subset=["latitude", "longitude"]).copy()
        if not point_df.empty:
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

    view_state = pdk.ViewState(latitude=32.75, longitude=-83.4, zoom=6.3, pitch=0)
    tooltip = {
        "html": (
            "<b>{county_name} County</b><br/>"
            "Total validated companies: {total_count}<br/>"
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
    st.caption("County polygons are authoritative boundaries. Point-radius uses geodesic distance; county distance uses polygon distance in EPSG:5070.")
    st.pydeck_chart(
        pdk.Deck(
            map_style="light_no_labels",
            initial_view_state=view_state,
            tooltip=tooltip,
            layers=layers,
        ),
        use_container_width=True,
    )


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

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if msg["role"] == "assistant":
            st.caption(f"Route: {msg.get('route_type', 'unknown')} | Mode: {msg.get('mode', 'unknown')}")
            render_sources(msg.get("sources", []))
            render_chunks(msg.get("retrieved_chunks", []))
            render_map(msg.get("retrieved_companies", []), st.session_state.get("selected_county"))
            render_table(msg.get("retrieved_companies", []))

user_question = st.chat_input("Ask a question about GNEM companies and geospatial relationships...")

if user_question:
    st.session_state.messages.append({"role": "user", "content": user_question})
    with st.chat_message("user"):
        st.markdown(user_question)

    with st.chat_message("assistant"):
        try:
            with st.spinner("Running deterministic geo/analytic routing plus evidence retrieval..."):
                result = call_backend(
                    user_question,
                    None if st.session_state.get("selected_county") == "All Counties" else st.session_state.get("selected_county"),
                )
            answer = result.get("answer", "No answer returned.")
            sources = result.get("sources", [])
            retrieved_chunks = result.get("retrieved_chunks", [])
            retrieved_companies = result.get("retrieved_companies", [])
            model_used = result.get("model_used", "unknown")
            route_type = result.get("route_type", "unknown")
            mode = result.get("mode", "unknown")

            st.markdown(answer)
            st.caption(f"Model: {model_used} | Route: {route_type} | Mode: {mode}")
            render_sources(sources)
            render_chunks(retrieved_chunks)
            render_map(retrieved_companies, st.session_state.get("selected_county"))
            render_table(retrieved_companies)

            st.session_state.messages.append(
                {
                    "role": "assistant",
                    "content": answer,
                    "sources": sources,
                    "retrieved_chunks": retrieved_chunks,
                    "retrieved_companies": retrieved_companies,
                    "route_type": route_type,
                    "mode": mode,
                }
            )
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
