from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Optional

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WORKBOOK_PATH = PROJECT_ROOT / "data" / "GNEM - Auto Landscape Lat Long Updated.xlsx"
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "data" / "address_city_county_extracted.xlsx"

# Example matched pattern:
# "975 Thomson Hwy, Warrenton, GA 30828"
ADDRESS_CITY_STATE_ZIP_RE = re.compile(
    r",\s*(?P<city>[^,]+?)\s*,\s*(?P<state>[A-Z]{2})\s*(?P<zip>\d{5}(?:-\d{4})?)?\s*$"
)

# Fallback for looser strings such as:
# "Bainbridge, GA (street address not publicly confirmed)"
ADDRESS_CITY_STATE_RE = re.compile(r"(?P<city>[A-Za-z][A-Za-z\s\-.']+?),\s*(?P<state>[A-Z]{2})\b")

# This only works when the address text literally contains "County".
COUNTY_IN_ADDRESS_RE = re.compile(r"\b(?P<county>[A-Za-z][A-Za-z\s\-.']+?)\s+County\b", re.IGNORECASE)


def extract_address_details(address: object) -> dict:
    if pd.isna(address):
        return {
            "extracted_city_from_address_regex": None,
            "extracted_county_from_address_regex": None,
            "extracted_state_from_address_regex": None,
            "address_regex_method": "missing_address",
        }

    text = str(address).strip()
    county_match = COUNTY_IN_ADDRESS_RE.search(text)
    extracted_county = county_match.group("county").strip().title() if county_match else None

    city_match = ADDRESS_CITY_STATE_ZIP_RE.search(text)
    if city_match:
        return {
            "extracted_city_from_address_regex": city_match.group("city").strip().title(),
            "extracted_county_from_address_regex": extracted_county,
            "extracted_state_from_address_regex": city_match.group("state").strip().upper(),
            "address_regex_method": "city_state_zip",
        }

    fallback_match = ADDRESS_CITY_STATE_RE.search(text)
    if fallback_match:
        return {
            "extracted_city_from_address_regex": fallback_match.group("city").strip().title(),
            "extracted_county_from_address_regex": extracted_county,
            "extracted_state_from_address_regex": fallback_match.group("state").strip().upper(),
            "address_regex_method": "city_state_fallback",
        }

    return {
        "extracted_city_from_address_regex": None,
        "extracted_county_from_address_regex": extracted_county,
        "extracted_state_from_address_regex": None,
        "address_regex_method": "no_regex_match",
    }


def build_address_extraction_table(workbook_path: Path) -> pd.DataFrame:
    df = pd.read_excel(workbook_path).rename(
        columns={
            "Company": "company_name",
            "Address": "address_raw",
        }
    )

    extracted = df["address_raw"].apply(extract_address_details).apply(pd.Series)
    result = pd.concat([df, extracted], axis=1)

    ordered_columns = [
        "company_name",
        "address_raw",
        "extracted_city_from_address_regex",
        "extracted_county_from_address_regex",
        "extracted_state_from_address_regex",
        "address_regex_method",
    ]

    keep_columns = [col for col in ordered_columns if col in result.columns]
    return result[keep_columns].sort_values(["company_name", "address_raw"], ascending=[True, True])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract city and county details from the Address column using regular expressions."
    )
    parser.add_argument("--workbook", type=Path, default=DEFAULT_WORKBOOK_PATH, help="Path to the Excel workbook.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH, help="Output Excel path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    table = build_address_extraction_table(args.workbook)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    table.to_excel(args.output, index=False)
    print(f"[address-regex] workbook={args.workbook}")
    print(f"[address-regex] rows={len(table)}")
    print(f"[address-regex] output={args.output}")


if __name__ == "__main__":
    main()
