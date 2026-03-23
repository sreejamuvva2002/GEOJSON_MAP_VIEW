from __future__ import annotations

import json
import os
import socket
import subprocess
import time
import urllib.error
import urllib.request
import urllib.parse
from pathlib import Path
from typing import Dict, List

import pandas as pd
import pydeck as pdk
import streamlit as st

st.set_page_config(page_title="Hybrid Geospatial RAG Chatbot", layout="wide")

BACKEND_CHAT_URL = os.getenv("BACKEND_CHAT_URL", "http://127.0.0.1:8000/chat")
PROJECT_ROOT = Path(__file__).resolve().parents[1]
CHAT_TIMEOUT_SECONDS = int(os.getenv("BACKEND_CHAT_TIMEOUT", "120"))
BACKEND_STARTUP_TIMEOUT_SECONDS = int(os.getenv("BACKEND_STARTUP_TIMEOUT", "35"))
AUTO_BACKEND_PORTS = [8000, 8001, 8002]


def health_url_from_chat_url(chat_url: str) -> str:
    if chat_url.endswith("/chat"):
        return chat_url[: -len("/chat")] + "/health"
    return chat_url.rstrip("/") + "/health"


def backend_is_healthy(chat_url: str) -> bool:
    health_url = health_url_from_chat_url(chat_url)
    try:
        with urllib.request.urlopen(health_url, timeout=3) as resp:
            if resp.status != 200:
                return False
            payload = json.loads(resp.read().decode("utf-8"))
            return bool(payload.get("pipeline_loaded"))
    except Exception:
        return False


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
    try:
        subprocess.Popen(
            ["python", "-m", "uvicorn", "backend.main:app", "--host", "127.0.0.1", "--port", str(int(port))],
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=_windows_creationflags(),
        )
    except Exception:
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

    # Try booting backend on preferred/default ports.
    ports = []
    preferred_port = parse_port_from_url(preferred)
    if preferred_port:
        ports.append(preferred_port)
    for p in AUTO_BACKEND_PORTS:
        if p not in ports:
            ports.append(p)

    # Attempt at most two start tries to avoid long UI blocking.
    attempts = 0
    for port in ports:
        if attempts >= 2:
            break
        url = chat_url_from_port(port)
        if is_port_listening(port) and not backend_is_healthy(url):
            continue
        attempts += 1
        if start_backend_on_port(port):
            st.session_state["backend_url"] = url
            return url

    raise urllib.error.URLError(
        "Backend is not reachable. Start it manually with: uvicorn backend.main:app --reload --port 8000"
    )


def call_backend(question: str) -> Dict:
    backend_url = discover_backend_url()

    payload = json.dumps({"question": question}).encode("utf-8")
    req = urllib.request.Request(
        backend_url,
        data=payload,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=CHAT_TIMEOUT_SECONDS) as resp:
        return json.loads(resp.read().decode("utf-8"))


def render_sources(sources: List[str]) -> None:
    if not sources:
        return
    st.markdown("**Retrieved Source Chunks**")
    for source in sources:
        st.markdown(f"- {source}")


def render_chunks(chunks: List[Dict]) -> None:
    if not chunks:
        return
    st.markdown("**Retrieved Chunks (Detailed)**")
    df = pd.DataFrame(chunks)
    preferred_cols = ["chunk_id", "engine", "company", "chunk_type", "score", "text"]
    ordered = [c for c in preferred_cols if c in df.columns] + [c for c in df.columns if c not in preferred_cols]
    st.dataframe(df[ordered], use_container_width=True)


def render_table(records: List[Dict]) -> None:
    if not records:
        return
    st.markdown("**Retrieved Companies**")
    df = pd.DataFrame(records)
    preferred_cols = [
        "company",
        "industry_group",
        "city",
        "county",
        "primary_oems",
        "map_weight",
        "employment",
        "distance_km",
        "coordinate_source",
        "score",
    ]
    ordered = [c for c in preferred_cols if c in df.columns] + [c for c in df.columns if c not in preferred_cols]
    st.dataframe(df[ordered], use_container_width=True)


def render_map(records: List[Dict]) -> None:
    if not records:
        return

    df = pd.DataFrame(records).copy()
    if "latitude" not in df.columns or "longitude" not in df.columns:
        return

    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df.dropna(subset=["latitude", "longitude"]).copy()
    if df.empty:
        return

    if "map_weight" not in df.columns:
        score_fallback = pd.to_numeric(df.get("score"), errors="coerce").fillna(0.5)
        df["map_weight"] = score_fallback.clip(lower=0.05, upper=1.0)
    else:
        df["map_weight"] = pd.to_numeric(df["map_weight"], errors="coerce").fillna(0.5).clip(lower=0.05, upper=1.0)

    df["radius"] = df["map_weight"].apply(lambda v: 4000.0 + float(v) * 26000.0)
    df["heat_weight"] = df["map_weight"].apply(lambda v: 5.0 + float(v) * 95.0)
    if "coordinate_source" not in df.columns:
        df["coordinate_source"] = "unknown"
    df["tooltip_company"] = df.get("company", pd.Series(index=df.index)).fillna("Unknown company")
    df["tooltip_role"] = df.get("ev_supply_chain_role", pd.Series(index=df.index)).fillna("Unknown role")
    df["tooltip_location"] = (
        df.get("city", pd.Series(index=df.index)).fillna("")
        + ", "
        + df.get("county", pd.Series(index=df.index)).fillna("")
    ).str.strip(", ")
    df["tooltip_coord_source"] = df.get("coordinate_source", pd.Series(index=df.index)).fillna("unknown")
    df["tooltip_map_weight"] = df["map_weight"].apply(lambda v: f"{float(v):.2f}")
    df["tooltip_map_reason"] = df.get("map_weight_reason", pd.Series(index=df.index)).fillna("No map weighting metadata")

    color_lookup = {
        "coordinates_excel": [32, 128, 141, 200],
        "source_excel": [54, 95, 145, 200],
        "county_centroid": [232, 148, 67, 190],
        "missing": [180, 60, 60, 160],
    }

    def _point_color(source: object) -> List[int]:
        text = str(source or "").lower()
        if text.startswith("coordinates_excel"):
            return color_lookup["coordinates_excel"]
        return color_lookup.get(text, [85, 98, 112, 180])

    df["fill_color"] = df["coordinate_source"].apply(_point_color)

    view_state = pdk.ViewState(
        latitude=float(df["latitude"].mean()),
        longitude=float(df["longitude"].mean()),
        zoom=6.5 if len(df) > 8 else 8.0,
        pitch=0,
    )

    heatmap_layer = pdk.Layer(
        "HeatmapLayer",
        data=df,
        get_position="[longitude, latitude]",
        get_weight="heat_weight",
        radius_pixels=45,
        intensity=1.0,
        threshold=0.05,
        opacity=0.55,
    )

    scatter_layer = pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position="[longitude, latitude]",
        get_fill_color="fill_color",
        get_line_color=[28, 33, 43, 180],
        line_width_min_pixels=1,
        stroked=True,
        filled=True,
        pickable=True,
        get_radius="radius",
    )

    tooltip = {
        "html": (
            "<b>{tooltip_company}</b><br/>"
            "Role: {tooltip_role}<br/>"
            "Location: {tooltip_location}<br/>"
            "Map score: {tooltip_map_weight}<br/>"
            "Coordinate source: {tooltip_coord_source}<br/>"
            "Why: {tooltip_map_reason}"
        ),
        "style": {"backgroundColor": "#16212b", "color": "white"},
    }

    st.markdown("**Supplier Coordinate Map**")
    st.caption("Heat intensity and marker size are weighted by query relevance, proximity, and business priority.")
    st.pydeck_chart(
        pdk.Deck(
            map_style="light_no_labels",
            initial_view_state=view_state,
            tooltip=tooltip,
            layers=[heatmap_layer, scatter_layer],
        ),
        use_container_width=True,
    )


st.title("Hybrid Geospatial RAG Chatbot")
st.caption("Ask questions over GNEM company data using SQL + geospatial filtering + semantic search.")

with st.sidebar:
    st.markdown("### Backend")
    effective_url = st.session_state.get("backend_url", BACKEND_CHAT_URL)
    st.code(effective_url, language="text")
    backend_ok = backend_is_healthy(effective_url)
    st.markdown(f"Status: {'Healthy' if backend_ok else 'Not reachable'}")
    if not backend_ok:
        st.caption("UI will auto-attempt to start backend on first chat request.")
        if st.button("Start Backend Now"):
            try:
                resolved = discover_backend_url()
                st.session_state["backend_url"] = resolved
                st.success(f"Backend ready at {resolved}")
            except Exception as exc:
                st.error(str(exc))
    st.markdown("### Example Questions")
    st.markdown("- Which EV suppliers are near Atlanta?")
    st.markdown("- List battery companies within 100 km of 33.7490, -84.3880.")
    st.markdown("- Top companies by employment.")
    st.markdown("- Which companies supply Ford?")

if "messages" not in st.session_state:
    st.session_state.messages = []

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if msg["role"] == "assistant":
            render_sources(msg.get("sources", []))
            render_chunks(msg.get("retrieved_chunks", []))
            render_map(msg.get("retrieved_companies", []))
            render_table(msg.get("retrieved_companies", []))

user_question = st.chat_input("Ask a question about GNEM companies and geospatial relationships...")

if user_question:
    st.session_state.messages.append({"role": "user", "content": user_question})
    with st.chat_message("user"):
        st.markdown(user_question)

    with st.chat_message("assistant"):
        try:
            with st.spinner("Retrieving from SQL + Geo + Vector engines..."):
                result = call_backend(user_question)
            answer = result.get("answer", "No answer returned.")
            sources = result.get("sources", [])
            retrieved_chunks = result.get("retrieved_chunks", [])
            retrieved_companies = result.get("retrieved_companies", [])
            model_used = result.get("model_used", "unknown")

            st.markdown(answer)
            st.caption(f"Model: {model_used}")
            render_sources(sources)
            render_chunks(retrieved_chunks)
            render_map(retrieved_companies)
            render_table(retrieved_companies)

            st.session_state.messages.append(
                {
                    "role": "assistant",
                    "content": answer,
                    "sources": sources,
                    "retrieved_chunks": retrieved_chunks,
                    "retrieved_companies": retrieved_companies,
                }
            )
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            st.error(f"Backend HTTP error: {exc.code} - {detail}")
        except urllib.error.URLError as exc:
            st.error(f"Backend connection failed: {exc.reason}")
        except Exception as exc:
            st.error(f"Unexpected error: {exc}")
