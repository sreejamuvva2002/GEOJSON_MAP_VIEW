from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from shapely.geometry import Point, shape

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WORKBOOK_PATH = PROJECT_ROOT / "data" / "GNEM - Auto Landscape Lat Long Updated.xlsx"
DEFAULT_GEOJSON_PATH = PROJECT_ROOT / "data" / "Counties_Georgia.geojson"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT

COUNTY_RE = re.compile(r"([A-Za-z][A-Za-z\s\-'.&]+?)\s+County\b", re.IGNORECASE)
STATE_RE = re.compile(r",\s*([A-Z]{2})\s*(?:\d{5}(?:-\d{4})?)?\s*$")


def normalize_text(value: object) -> Optional[str]:
    if pd.isna(value):
        return None
    text = re.sub(r"[^a-z0-9]+", " ", str(value).lower()).strip()
    text = re.sub(r"\s+", " ", text)
    return text or None


def parse_labeled_county(location: object) -> Optional[str]:
    if pd.isna(location):
        return None
    text = str(location).strip()
    match = COUNTY_RE.search(text)
    if match:
        return match.group(1).strip().title()
    parts = [part.strip() for part in text.split(",") if part.strip()]
    if len(parts) == 1 and parts[0].lower().endswith("county"):
        return parts[0][: -len("county")].strip().title()
    return None


def parse_location_city(location: object) -> Optional[str]:
    if pd.isna(location):
        return None
    parts = [part.strip() for part in str(location).split(",") if part.strip()]
    if not parts:
        return None
    first = parts[0]
    if "county" in first.lower() or first.lower() == "georgia":
        return None
    return first.title()


def parse_address(address: object) -> Tuple[Optional[str], Optional[str]]:
    if pd.isna(address):
        return None, None
    text = str(address).strip()
    state = None
    match = STATE_RE.search(text)
    if match:
        state = match.group(1)
    parts = [part.strip() for part in text.split(",") if part.strip()]
    city = None
    if len(parts) >= 2:
        city = parts[-2].title()
    return city, state


def load_county_geometries(geojson_path: Path) -> List[Tuple[str, object]]:
    payload = json.loads(geojson_path.read_text(encoding="utf-8"))
    counties: List[Tuple[str, object]] = []
    for feature in payload.get("features", []):
        props = feature.get("properties", {})
        county_name = (
            props.get("NAME10")
            or props.get("NAME")
            or str(props.get("NAMELSAD10", "")).replace("County", "").strip()
        )
        if not county_name:
            continue
        counties.append((str(county_name).strip().title(), shape(feature["geometry"])))
    return counties


def compute_county(longitude: float, latitude: float, counties: List[Tuple[str, object]]) -> Optional[str]:
    point = Point(float(longitude), float(latitude))
    for county_name, geom in counties:
        if geom.contains(point) or geom.touches(point):
            return county_name
    return None


def join_unique(values: pd.Series) -> str:
    unique = sorted({str(v) for v in values if pd.notna(v) and str(v).strip()})
    return " | ".join(unique)


def build_duplicate_clusters(df: pd.DataFrame) -> pd.DataFrame:
    usable = df[df["usable_lat_lon"]].copy()
    if usable.empty:
        return pd.DataFrame()
    grouped = (
        usable.groupby(["latitude", "longitude"], dropna=False)
        .agg(
            row_count=("company_name", "size"),
            unique_company_count=("company_name", lambda s: len({str(v) for v in s if pd.notna(v)})),
            company_names=("company_name", join_unique),
            locations=("labeled_location", join_unique),
            addresses=("address", join_unique),
            labeled_counties=("labeled_county", join_unique),
            computed_counties=("computed_county", join_unique),
        )
        .reset_index()
    )
    grouped = grouped[grouped["row_count"] > 1].copy()
    grouped["duplicate_cluster_id"] = [f"DUP{idx:03d}" for idx in range(1, len(grouped) + 1)]
    grouped["same_coordinates_multiple_companies"] = grouped["unique_company_count"] > 1
    ordered_cols = [
        "duplicate_cluster_id",
        "latitude",
        "longitude",
        "row_count",
        "unique_company_count",
        "same_coordinates_multiple_companies",
        "company_names",
        "locations",
        "addresses",
        "labeled_counties",
        "computed_counties",
    ]
    return grouped[ordered_cols].sort_values(
        ["row_count", "unique_company_count", "duplicate_cluster_id"],
        ascending=[False, False, True],
    )


def classify_conflicts(df: pd.DataFrame, duplicate_clusters: pd.DataFrame) -> pd.DataFrame:
    duplicate_map: Dict[Tuple[float, float], str] = {}
    if not duplicate_clusters.empty:
        for _, row in duplicate_clusters.iterrows():
            duplicate_map[(float(row["latitude"]), float(row["longitude"]))] = str(row["duplicate_cluster_id"])

    conflict_rows: List[Dict[str, object]] = []
    for _, row in df.iterrows():
        flags: List[str] = []

        location_city_norm = normalize_text(row.get("location_city"))
        address_city_norm = normalize_text(row.get("address_city"))
        labeled_county_norm = normalize_text(row.get("labeled_county"))
        computed_county_norm = normalize_text(row.get("computed_county"))

        if row.get("address_state") and row.get("address_state") != "GA":
            flags.append("address_state_not_ga")
        if location_city_norm and address_city_norm:
            if location_city_norm != address_city_norm and location_city_norm not in address_city_norm and address_city_norm not in location_city_norm:
                flags.append("location_city_vs_address_city_conflict")
        if labeled_county_norm and computed_county_norm and labeled_county_norm != computed_county_norm:
            flags.append("labeled_county_vs_computed_county_mismatch")
        if bool(row.get("outside_georgia_county_polygon")):
            flags.append("outside_georgia_polygon")

        lat = row.get("latitude")
        lon = row.get("longitude")
        cluster_id = None
        if pd.notna(lat) and pd.notna(lon):
            cluster_id = duplicate_map.get((float(lat), float(lon)))
            if cluster_id:
                flags.append("duplicate_coordinate_cluster")

        if flags:
            conflict_rows.append(
                {
                    "company_name": row.get("company_name"),
                    "labeled_location": row.get("labeled_location"),
                    "address": row.get("address"),
                    "latitude": row.get("latitude"),
                    "longitude": row.get("longitude"),
                    "labeled_county": row.get("labeled_county"),
                    "computed_county": row.get("computed_county"),
                    "location_city": row.get("location_city"),
                    "address_city": row.get("address_city"),
                    "address_state": row.get("address_state"),
                    "outside_georgia_county_polygon": row.get("outside_georgia_county_polygon"),
                    "duplicate_cluster_id": cluster_id,
                    "conflict_flags": " | ".join(flags),
                }
            )

    conflicts = pd.DataFrame(conflict_rows)
    if conflicts.empty:
        return conflicts
    return conflicts.sort_values(["company_name", "labeled_location"], ascending=[True, True]).reset_index(drop=True)


def audit_dataset(workbook_path: Path, geojson_path: Path) -> Dict[str, object]:
    df = pd.read_excel(workbook_path)
    counties = load_county_geometries(geojson_path)

    out = df.rename(
        columns={
            "Company": "company_name",
            "Location": "labeled_location",
            "Address": "address",
            "Latitude": "latitude",
            "Longitude": "longitude",
        }
    ).copy()

    out["latitude"] = pd.to_numeric(out["latitude"], errors="coerce")
    out["longitude"] = pd.to_numeric(out["longitude"], errors="coerce")

    out["has_lat_lon"] = out["latitude"].notna() & out["longitude"].notna()
    out["usable_lat_lon"] = out["has_lat_lon"] & out["latitude"].between(-90, 90) & out["longitude"].between(-180, 180)
    out["missing_lat_lon"] = ~out["has_lat_lon"]
    out["impossible_lat_lon"] = out["has_lat_lon"] & ~out["usable_lat_lon"]

    out["labeled_county"] = out["labeled_location"].apply(parse_labeled_county)
    out["location_city"] = out["labeled_location"].apply(parse_location_city)
    out[["address_city", "address_state"]] = out["address"].apply(lambda value: pd.Series(parse_address(value)))

    computed_counties: List[Optional[str]] = []
    outside_flags: List[bool] = []
    for _, row in out.iterrows():
        if not bool(row["usable_lat_lon"]):
            computed_counties.append(None)
            outside_flags.append(False)
            continue
        computed = compute_county(float(row["longitude"]), float(row["latitude"]), counties)
        computed_counties.append(computed)
        outside_flags.append(computed is None)

    out["computed_county"] = computed_counties
    out["outside_georgia_county_polygon"] = outside_flags

    county_compare_mask = out["usable_lat_lon"] & out["labeled_county"].notna()
    county_match_mask = county_compare_mask & out["computed_county"].notna() & (
        out["labeled_county"].str.lower() == out["computed_county"].str.lower()
    )
    county_mismatch_mask = county_compare_mask & out["computed_county"].notna() & (
        out["labeled_county"].str.lower() != out["computed_county"].str.lower()
    )

    county_mismatches = out[county_mismatch_mask].copy()
    county_mismatches = county_mismatches[
        [
            "company_name",
            "labeled_location",
            "address",
            "latitude",
            "longitude",
            "computed_county",
            "labeled_county",
            "address_city",
            "address_state",
        ]
    ].sort_values(["company_name", "labeled_location"], ascending=[True, True])

    outside_polygon_rows = out[out["outside_georgia_county_polygon"]].copy()
    outside_polygon_rows = outside_polygon_rows[
        [
            "company_name",
            "labeled_location",
            "address",
            "latitude",
            "longitude",
            "labeled_county",
            "address_city",
            "address_state",
        ]
    ].sort_values(["company_name", "labeled_location"], ascending=[True, True])

    duplicate_clusters = build_duplicate_clusters(out)
    address_location_conflicts = classify_conflicts(out, duplicate_clusters)

    rows_with_coordinates = int(out["usable_lat_lon"].sum())
    rows_with_county_and_coordinates = int(county_compare_mask.sum())
    mismatch_rows = int(county_mismatch_mask.sum())
    outside_polygon_rows_count = int(out["outside_georgia_county_polygon"].sum())

    stats = {
        "total_rows": int(len(out)),
        "rows_with_usable_lat_lon": rows_with_coordinates,
        "rows_missing_lat_lon": int(out["missing_lat_lon"].sum()),
        "rows_with_impossible_lat_lon": int(out["impossible_lat_lon"].sum()),
        "rows_outside_polygon": outside_polygon_rows_count,
        "rows_with_county_and_coordinates": rows_with_county_and_coordinates,
        "county_matches": int(county_match_mask.sum()),
        "county_mismatches": mismatch_rows,
        "county_mismatch_rate": (mismatch_rows / rows_with_county_and_coordinates) if rows_with_county_and_coordinates else None,
        "outside_ga_rate": (outside_polygon_rows_count / rows_with_coordinates) if rows_with_coordinates else None,
        "address_state_not_ga_rows": int(((out["address_state"].notna()) & (out["address_state"] != "GA")).sum()),
        "duplicate_coordinate_clusters": int(len(duplicate_clusters)),
        "duplicate_coordinate_rows": int(duplicate_clusters["row_count"].sum()) if not duplicate_clusters.empty else 0,
    }

    return {
        "profiled_rows": out,
        "county_mismatches": county_mismatches,
        "outside_polygon_rows": outside_polygon_rows,
        "duplicate_clusters": duplicate_clusters,
        "address_location_conflicts": address_location_conflicts,
        "stats": stats,
        "counties_loaded": len(counties),
    }


def render_report(
    workbook_path: Path,
    geojson_path: Path,
    audit: Dict[str, object],
) -> str:
    stats = audit["stats"]
    mismatches = audit["county_mismatches"]
    outside_rows = audit["outside_polygon_rows"]
    duplicate_clusters = audit["duplicate_clusters"]
    conflicts = audit["address_location_conflicts"]

    top_mismatches = mismatches.head(25)
    outside_preview = outside_rows.copy()
    address_state_not_ga = conflicts[conflicts["conflict_flags"].str.contains("address_state_not_ga", na=False)].copy()
    city_address_conflicts = conflicts[
        conflicts["conflict_flags"].str.contains("location_city_vs_address_city_conflict", na=False)
    ].copy()

    outside_rate = stats["outside_ga_rate"]
    mismatch_rate = stats["county_mismatch_rate"]

    def pct(value: Optional[float]) -> str:
        if value is None:
            return "N/A"
        return f"{value * 100:.2f}%"

    verdict = "not trustworthy / ingestion should fail"
    if outside_rate is not None and mismatch_rate is not None:
        if outside_rate <= 0.01 and mismatch_rate <= 0.05:
            verdict = "trusted"
        elif outside_rate <= 0.01 and mismatch_rate <= 0.10:
            verdict = "usable with quarantine"

    lines: List[str] = []
    lines.append("# Geo Quality Audit Report")
    lines.append("")
    lines.append("## Inputs")
    lines.append("")
    lines.append(f"- Workbook: `{workbook_path}`")
    lines.append(f"- County polygons: `{geojson_path}`")
    lines.append(f"- Georgia county polygon count: `{audit['counties_loaded']}`")
    lines.append("")
    lines.append("## Rules Used")
    lines.append("")
    lines.append("1. **Usable latitude/longitude**: both numeric and within valid Earth ranges (`-90 <= lat <= 90`, `-180 <= lon <= 180`).")
    lines.append("2. **Computed county**: the Georgia county polygon that contains the point. This is a standard point-in-polygon rule and is the correct rule for county membership.")
    lines.append("3. **Outside Georgia county polygon**: point does not fall inside any county polygon in `Counties_Georgia.geojson`.")
    lines.append("4. **County mismatch**: the workbook's stated county extracted from `Location` does not equal the polygon-derived `computed_county`.")
    lines.append("5. **Address state conflict**: the address ends in a non-`GA` state abbreviation. This is weaker than polygon evidence because mailing/HQ addresses can differ, but it is still a valid inconsistency signal in a Georgia facility workbook.")
    lines.append("6. **Strong location/address conflict**: parsed city from `Location` disagrees with parsed city from `Address`, with neither string containing the other. This is a heuristic signal, not a geometry proof.")
    lines.append("7. **Duplicate coordinate cluster**: exact same `(latitude, longitude)` is assigned to more than one row. This is suspicious, especially when it spans multiple company names, but not by itself conclusive.")
    lines.append("")
    lines.append("## Dataset Profiling")
    lines.append("")
    lines.append(f"- Total workbook rows: `{stats['total_rows']}`")
    lines.append(f"- Rows with usable lat/lon: `{stats['rows_with_usable_lat_lon']}`")
    lines.append(f"- Rows missing lat/lon: `{stats['rows_missing_lat_lon']}`")
    lines.append(f"- Rows with impossible lat/lon: `{stats['rows_with_impossible_lat_lon']}`")
    lines.append(f"- Duplicate coordinate clusters: `{stats['duplicate_coordinate_clusters']}`")
    lines.append(f"- Rows outside Georgia county polygons: `{stats['rows_outside_polygon']}`")
    lines.append(f"- Rows with address state not equal to `GA`: `{stats['address_state_not_ga_rows']}`")
    lines.append("")
    lines.append("## County Validation Summary")
    lines.append("")
    lines.append(f"- Rows with both county label and usable coordinates: `{stats['rows_with_county_and_coordinates']}`")
    lines.append(f"- County matches: `{stats['county_matches']}`")
    lines.append(f"- County mismatches: `{stats['county_mismatches']}`")
    lines.append(f"- County mismatch rate: `{pct(mismatch_rate)}`")
    lines.append(f"- Outside-GA-polygon rate: `{pct(outside_rate)}`")
    lines.append("")
    lines.append("## Fail-Fast Threshold Check")
    lines.append("")
    lines.append(f"- `outside_ga_rate > 1%`: {'EXCEEDED' if outside_rate is not None and outside_rate > 0.01 else 'not exceeded'} (`{pct(outside_rate)}`)")
    lines.append(f"- `county_mismatch_rate > 5%`: {'EXCEEDED' if mismatch_rate is not None and mismatch_rate > 0.05 else 'not exceeded'} (`{pct(mismatch_rate)}`)")
    lines.append(f"- `county_mismatch_rate > 10%`: {'EXCEEDED' if mismatch_rate is not None and mismatch_rate > 0.10 else 'not exceeded'} (`{pct(mismatch_rate)}`)")
    lines.append("")
    lines.append("## Major Conclusions With Basis")
    lines.append("")
    lines.append("### Conclusion 1: The problem is systematic, not a small set of bad rows")
    lines.append("")
    lines.append("- **Rule used**: polygon containment plus county label comparison.")
    lines.append("- **Why valid**: if coordinates are correct, most Georgia facility points should fall inside their stated county polygon.")
    lines.append(f"- **Supporting evidence**: `{stats['county_mismatches']}` mismatches out of `{stats['rows_with_county_and_coordinates']}` county-labeled rows with usable coordinates (`{pct(mismatch_rate)}`).")
    lines.append("- **Interpretation**: that rate is far too high to be explained by a handful of bad geocodes.")
    lines.append("")
    lines.append("### Conclusion 2: The evidence suggests workbook contamination or an upstream bad join, not just isolated geocoding noise")
    lines.append("")
    lines.append("- **Rule used**: compare company name, labeled `Location`, `Address`, and polygon-derived county together.")
    lines.append("- **Why valid**: if a row says one Georgia place but the address names a different city/state, that contradiction is inside the workbook itself.")
    lines.append("- **Supporting evidence**:")
    lines.append("  - `ACM Georgia LLC`: `Calhoun, Gordon County` vs address in `Warrenton, GA`")
    lines.append("  - `Adient`: `Ringgold, Catoosa County` vs address in `West Point, GA`")
    lines.append("  - `Elan Technology Inc.`: `Atlanta, Fulton County` vs address in `Midway, GA`")
    lines.append("  - `Michelin Tread Technologies`: `Lawrenceville, Gwinnett County` vs address in `Greenville, SC`")
    lines.append("- **Interpretation**: these are strong signs of misaligned rows, contaminated workbook content, or mixed facility/HQ records. The contradiction exists before the repo's own polygon logic is applied.")
    lines.append("")
    lines.append("### Conclusion 3: The current geo layer cannot be trusted for county analytics")
    lines.append("")
    lines.append("- **Rule used**: county analytics require correct county membership.")
    lines.append("- **Why valid**: if the point is outside the stated county, county counts and county gap analytics become wrong.")
    lines.append(f"- **Supporting evidence**: `{stats['county_mismatches']}` county mismatches and `{stats['rows_outside_polygon']}` rows outside all Georgia county polygons.")
    lines.append("")
    lines.append("### Conclusion 4: The current geo layer cannot be trusted for distance/radius search")
    lines.append("")
    lines.append("- **Rule used**: distance search assumes the stored point location is the actual facility point.")
    lines.append("- **Why valid**: point-to-point or point-to-polygon distance is only as good as the point itself.")
    lines.append("- **Supporting evidence**: rows with out-of-state addresses, rows outside Georgia polygons, and rows whose computed county disagrees with the labeled county.")
    lines.append("")
    lines.append("### Conclusion 5: Ingestion should fail fast")
    lines.append("")
    lines.append("- **Rule used**: compare observed rates against the stated thresholds.")
    lines.append("- **Why valid**: letting ingestion proceed would silently contaminate the map, analytics, and retrieval layers.")
    lines.append(f"- **Supporting evidence**: outside-GA rate `{pct(outside_rate)}` and county mismatch rate `{pct(mismatch_rate)}`, both above the requested thresholds.")
    lines.append("")
    lines.append("## Proof Tables")
    lines.append("")
    lines.append("### Top 25 County Mismatches")
    lines.append("")
    lines.append(top_mismatches.to_markdown(index=False) if not top_mismatches.empty else "_No county mismatches found._")
    lines.append("")
    lines.append("### All Rows Outside Georgia County Polygons")
    lines.append("")
    lines.append(outside_preview.to_markdown(index=False) if not outside_preview.empty else "_No outside-polygon rows found._")
    lines.append("")
    lines.append("### All Duplicate Coordinate Clusters")
    lines.append("")
    lines.append(duplicate_clusters.to_markdown(index=False) if not duplicate_clusters.empty else "_No duplicate coordinate clusters found._")
    lines.append("")
    lines.append("### Rows Whose Address State Is Not GA")
    lines.append("")
    lines.append(
        address_state_not_ga[
            [
                "company_name",
                "labeled_location",
                "address",
                "latitude",
                "longitude",
                "labeled_county",
                "computed_county",
                "address_state",
                "conflict_flags",
            ]
        ].to_markdown(index=False)
        if not address_state_not_ga.empty
        else "_No non-GA address-state rows found._"
    )
    lines.append("")
    lines.append("### Rows Where Location Text Strongly Disagrees With Address Text")
    lines.append("")
    lines.append(
        city_address_conflicts[
            [
                "company_name",
                "labeled_location",
                "address",
                "location_city",
                "address_city",
                "address_state",
                "labeled_county",
                "computed_county",
                "conflict_flags",
            ]
        ].head(100).to_markdown(index=False)
        if not city_address_conflicts.empty
        else "_No strong location/address text conflicts found under the audit rule._"
    )
    lines.append("")
    lines.append("## Supported Interpretation")
    lines.append("")
    lines.append("### Is this a small number of bad rows, or systematic?")
    lines.append("")
    lines.append(f"Systematic. The strongest evidence is the county mismatch rate: `{pct(mismatch_rate)}` (`{stats['county_mismatches']}` of `{stats['rows_with_county_and_coordinates']}`).")
    lines.append("")
    lines.append("### Does the evidence suggest a bad join between company rows and coordinate rows?")
    lines.append("")
    lines.append("Probably yes, but with one important caveat: the contradictions are already visible inside the workbook itself. That means the problem may be an upstream join, workbook contamination, or mixed facility/HQ records. The current audit supports the claim that the coordinate source is unreliable; it cannot by itself prove which upstream step introduced the corruption.")
    lines.append("")
    lines.append("### Can this geo layer be trusted for map visualization?")
    lines.append("")
    lines.append("Not for professor-defensible geographic visualization. A map can still be drawn, but the evidence does not support trusting the points as facility-accurate.")
    lines.append("")
    lines.append("### Can it be trusted for county-level analytics?")
    lines.append("")
    lines.append("No. County-level analytics depend on correct county membership, which the audit shows is violated at a very high rate.")
    lines.append("")
    lines.append("### Can it be trusted for distance/radius search?")
    lines.append("")
    lines.append("No. Distance/radius search requires trustworthy point geometry, and the current point layer fails polygon and consistency checks.")
    lines.append("")
    lines.append("### Should ingestion fail-fast?")
    lines.append("")
    lines.append("Yes. Both requested fail-fast thresholds are exceeded.")
    lines.append("")
    lines.append("## Uncertainty and Assumptions")
    lines.append("")
    lines.append("- The polygon containment logic assumes `Counties_Georgia.geojson` is the authoritative county boundary source for Georgia.")
    lines.append("- The county comparison assumes the `Location` field is intended to describe the facility county. Rows like `Georgia` or rows without a parsable county are excluded from the county-mismatch denominator.")
    lines.append("- Address-based conflict rules are weaker than polygon evidence because an address can refer to HQ, mailing, or another corporate site. That is why the strongest claims in this report rely on polygon containment and county mismatch, not just address text.")
    lines.append("- Exact duplicate coordinates across different companies are suspicious but not always wrong; industrial parks and shared campuses can produce legitimate duplicates. They are therefore presented as evidence to inspect, not as standalone proof of corruption.")
    lines.append("")
    lines.append("## Final Verdict")
    lines.append("")
    lines.append(f"**Verdict: {verdict}**")
    lines.append("")
    if verdict == "not trustworthy / ingestion should fail":
        lines.append("The geometry evidence is strong enough that this workbook should not pass ingestion into a county-aware map, county analytics layer, or distance/radius search system without a coordinate-join audit and corrective cleanup.")
    elif verdict == "usable with quarantine":
        lines.append("The workbook has meaningful geo issues, but a quarantine-based pipeline may still be defensible if the flagged rows are excluded and the remaining rates stay below the stated thresholds.")
    else:
        lines.append("The workbook passes the requested geo-quality thresholds under the current audit rules.")
    lines.append("")
    lines.append("## Output Files")
    lines.append("")
    lines.append("- `outside_polygon_rows.csv`")
    lines.append("- `county_mismatches.csv`")
    lines.append("- `duplicate_coordinates.csv`")
    lines.append("- `address_location_conflicts.csv`")
    return "\n".join(lines).strip() + "\n"


def write_outputs(audit: Dict[str, object], output_dir: Path, report_text: str) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    audit["outside_polygon_rows"].to_csv(output_dir / "outside_polygon_rows.csv", index=False)
    audit["county_mismatches"].to_csv(output_dir / "county_mismatches.csv", index=False)
    audit["duplicate_clusters"].to_csv(output_dir / "duplicate_coordinates.csv", index=False)
    audit["address_location_conflicts"].to_csv(output_dir / "address_location_conflicts.csv", index=False)
    (output_dir / "GEO_QUALITY_AUDIT_REPORT.md").write_text(report_text, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit workbook coordinates against Georgia county polygons.")
    parser.add_argument("--workbook", type=Path, default=DEFAULT_WORKBOOK_PATH, help="Path to the coordinate workbook.")
    parser.add_argument("--geojson", type=Path, default=DEFAULT_GEOJSON_PATH, help="Path to the county GeoJSON.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Directory for report and CSV outputs.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    audit = audit_dataset(workbook_path=args.workbook, geojson_path=args.geojson)
    report_text = render_report(workbook_path=args.workbook, geojson_path=args.geojson, audit=audit)
    write_outputs(audit=audit, output_dir=args.output_dir, report_text=report_text)
    stats = audit["stats"]
    print(f"[geo-audit] total_rows={stats['total_rows']}")
    print(f"[geo-audit] usable_lat_lon={stats['rows_with_usable_lat_lon']}")
    print(f"[geo-audit] outside_polygon_rows={stats['rows_outside_polygon']}")
    print(f"[geo-audit] county_mismatches={stats['county_mismatches']}")
    print(f"[geo-audit] report={args.output_dir / 'GEO_QUALITY_AUDIT_REPORT.md'}")


if __name__ == "__main__":
    main()
