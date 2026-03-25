from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from backend.geo_utils import file_sha256

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOG_DIR = PROJECT_ROOT / "logs"
DEFAULT_LOG_PATH = DEFAULT_LOG_DIR / "app_events.jsonl"


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def git_commit_hash(repo_root: Path = PROJECT_ROOT) -> Optional[str]:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip() or None
    except Exception:
        return None


def append_jsonl(event: Dict[str, Any], log_path: Path = DEFAULT_LOG_PATH) -> None:
    log_path = Path(log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"timestamp": utc_timestamp(), **event}
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, default=str, sort_keys=True) + "\n")


def build_runtime_metadata(
    *,
    planner_route: Optional[str],
    county: Optional[str],
    radius_miles: Optional[float],
    geo_anchor_type: Optional[str],
    evidence_ids: list[str],
    configured_crs: Optional[str],
    county_field_trusted: Optional[bool],
    retrieval_summary: Dict[str, Any],
    selected_model: Optional[str],
    embedding_backend: Optional[str],
    embedding_model: Optional[str],
    geojson_path: Optional[Path],
    excel_path: Optional[Path],
    answer_text: Optional[str],
    user_query: Optional[str],
    errors: Optional[str] = None,
) -> Dict[str, Any]:
    geojson_file = Path(geojson_path) if geojson_path else None
    excel_file = Path(excel_path) if excel_path else None
    return {
        "user_query": user_query,
        "planner_route": planner_route,
        "county": county,
        "radius_miles": radius_miles,
        "geo_anchor_type": geo_anchor_type,
        "evidence_ids": evidence_ids,
        "configured_crs": configured_crs,
        "COUNTY_FIELD_TRUSTED": county_field_trusted,
        "retrieval_summary": retrieval_summary,
        "selected_model": selected_model,
        "embedding_backend": embedding_backend,
        "embedding_model": embedding_model,
        "git_commit_hash": git_commit_hash(),
        "geojson_file_hash": file_sha256(geojson_file) if geojson_file else None,
        "excel_file_hash": file_sha256(excel_file) if excel_file and excel_file.exists() else None,
        "excel_file_mtime": excel_file.stat().st_mtime if excel_file and excel_file.exists() else None,
        "answer_text": answer_text,
        "errors": errors,
    }
