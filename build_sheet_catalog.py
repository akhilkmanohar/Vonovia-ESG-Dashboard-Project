from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine
from sqlalchemy.exc import IntegrityError, SQLAlchemyError


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "Data"

# Mapping of year to (category, workbook path) pairs
KEY_FIGURES_FILES: Dict[str, List[Tuple[str, Path]]] = {
    "2024": [
        ("Environment", DATA_DIR / "2024" / "VONOVIA_ESG_FB24_Key_Figures_Environment.xlsx"),
        ("Social", DATA_DIR / "2024" / "VONOVIA_ESG_FB24_Key_Figures-Social.xlsx"),
        ("Governance", DATA_DIR / "2024" / "VONOVIA_ESG_FB24_Key_Figures_Governance.xlsx"),
    ],
    "2023": [
        ("Environment", DATA_DIR / "2023" / "VONOVIA_-ESG_FB23_Key_Figures_Environment.xlsx"),
        ("Social", DATA_DIR / "2023" / "VONOVIA_-ESG_FB23_Key_Figures_Social.xlsx"),
        ("Governance", DATA_DIR / "2023" / "VONOVIA_-ESG_FB23_Key_Figures_Governance.xlsx"),
    ],
    "2022": [
        ("Environment", DATA_DIR / "2022" / "VONOVIA_-SR22_Key-Figures_Environmental.xlsx"),
        ("Social", DATA_DIR / "2022" / "VONOVIA_-SR22_Key-Figures_Social.xlsx"),
        ("Governance", DATA_DIR / "2022" / "VONOVIA_-SR22_Key-Figures_Governance.xlsx"),
    ],
}

SERVER_NAME = "AKHIL-K-MANOHAR"
DATABASE_NAME = "Vonovia_ESG_DB"
DRIVER_CANDIDATES = ["ODBC Driver 17 for SQL Server", "ODBC Driver 18 for SQL Server"]


def create_engine_with_fallback() -> Engine:
    """Create a SQLAlchemy engine, trying ODBC Driver 17 first then 18."""
    last_error: SQLAlchemyError | None = None
    for driver in DRIVER_CANDIDATES:
        connection_url = URL.create(
            "mssql+pyodbc",
            host=SERVER_NAME,
            database=DATABASE_NAME,
            query={"driver": driver, "trusted_connection": "yes"},
        )
        engine = create_engine(connection_url, fast_executemany=True)
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except SQLAlchemyError as exc:
            last_error = exc
            continue
        print(f"Connected to SQL Server using {driver}.")
        return engine
    assert last_error is not None  # for mypy/pyright
    raise last_error


def collect_catalog_entries() -> Iterable[Dict[str, str]]:
    """Yield catalog records for every sheet found in the key figure workbooks."""
    for year_label, file_entries in KEY_FIGURES_FILES.items():
        for category, workbook_path in file_entries:
            if not workbook_path.exists():
                raise FileNotFoundError(f"Workbook not found: {workbook_path}")
            source_file = workbook_path.name
            excel_file = pd.ExcelFile(workbook_path)
            for sheet_name in excel_file.sheet_names:
                yield {
                    "year_label": year_label,
                    "sheet_name": sheet_name,
                    "category": category,
                    "source_file": source_file,
                }


def upsert_catalog_entries(engine: Engine, entries: Iterable[Dict[str, str]]) -> Dict[Tuple[str, str], Dict[str, int]]:
    summary: Dict[Tuple[str, str], Dict[str, int]] = defaultdict(lambda: {"inserted": 0, "updated": 0, "unchanged": 0})
    entries_list = list(entries)
    if not entries_list:
        print("No entries collected; nothing to upsert.")
        return summary

    select_stmt = text(
        """
SELECT category, source_file
FROM core.sheet_catalog
WHERE year_label = :year_label
  AND sheet_name = :sheet_name;
"""
    )
    update_stmt = text(
        """
UPDATE core.sheet_catalog
SET category = :category,
    source_file = :source_file
WHERE year_label = :year_label
  AND sheet_name = :sheet_name;
"""
    )
    insert_stmt = text(
        """
INSERT INTO core.sheet_catalog (year_label, sheet_name, category, source_file)
VALUES (:year_label, :sheet_name, :category, :source_file);
"""
    )

    with engine.begin() as conn:
        for record in entries_list:
            bucket = summary[(record["year_label"], record["category"])]
            existing = conn.execute(select_stmt, record).fetchone()
            if existing:
                existing_category, existing_source = existing
                if (existing_category or "") == record["category"] and (existing_source or "") == record["source_file"]:
                    bucket["unchanged"] += 1
                    continue
                conn.execute(update_stmt, record)
                bucket["updated"] += 1
                continue
            try:
                conn.execute(insert_stmt, record)
                bucket["inserted"] += 1
            except IntegrityError:
                # Handle race condition: fetch fresh state and decide.
                existing = conn.execute(select_stmt, record).fetchone()
                if existing:
                    existing_category, existing_source = existing
                    if (existing_category or "") == record["category"] and (existing_source or "") == record["source_file"]:
                        bucket["unchanged"] += 1
                    else:
                        conn.execute(update_stmt, record)
                        bucket["updated"] += 1
                else:
                    raise
    return summary


def print_summary(summary: Dict[Tuple[str, str], Dict[str, int]]) -> None:
    if not summary:
        print("No changes made to core.sheet_catalog.")
        return
    print("\nSheet catalog upsert summary:")
    for (year_label, category) in sorted(summary):
        metrics = summary[(year_label, category)]
        print(
            f"  Year {year_label} | {category}: "
            f"{metrics['inserted']} inserted, {metrics['updated']} updated, {metrics['unchanged']} unchanged"
        )


def main() -> None:
    engine = create_engine_with_fallback()
    entries = collect_catalog_entries()
    summary = upsert_catalog_entries(engine, entries)
    print_summary(summary)


if __name__ == "__main__":
    main()
