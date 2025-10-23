from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine
from sqlalchemy.exc import SQLAlchemyError


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "Data"

ALL_TABLE_WORKBOOKS: Dict[str, Tuple[str, Path]] = {
    "2024": ("stg.raw_fb24_all", DATA_DIR / "2024" / "VONOVIA_ESG_FB24_All_Tables.xlsx"),
    "2023": ("stg.raw_fb23_all", DATA_DIR / "2023" / "VONOVIA_ESG_FB23_All_Tables.xlsx"),
    "2022": ("stg.raw_sr22_all", DATA_DIR / "2022" / "VONOVIA_SR22_All_Tables.xlsx"),
}

SERVER_NAME = "AKHIL-K-MANOHAR"
DATABASE_NAME = "Vonovia_ESG_DB"
DRIVER_CANDIDATES = ["ODBC Driver 17 for SQL Server", "ODBC Driver 18 for SQL Server"]

COLUMNS: List[str] = [f"c{i:02d}" for i in range(1, 29)]
INBOX_TABLE = ("stg", "py_inbox")


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
    assert last_error is not None
    raise last_error


def quoted_table(schema: str, name: str) -> str:
    return f"[{schema}].[{name}]"


def ensure_py_inbox(engine: Engine) -> None:
    column_defs = ",\n        ".join(f"{col} NVARCHAR(MAX) NULL" for col in COLUMNS)
    create_sql = f"""
IF OBJECT_ID('{quoted_table(*INBOX_TABLE)}', 'U') IS NULL
BEGIN
    CREATE TABLE {quoted_table(*INBOX_TABLE)} (
        sheet_name NVARCHAR(255) NOT NULL,
        row_num INT NOT NULL,
        {column_defs},
        source_file NVARCHAR(255) NOT NULL
    );
END;
"""
    with engine.begin() as conn:
        conn.execute(text(create_sql))


def normalize_sheet_name(sheet_name: str | None) -> str:
    if sheet_name is None:
        return ""
    return sheet_name.strip().lower()


def load_catalog_lookup(engine: Engine) -> Dict[str, Dict[Tuple[str, str], str]]:
    lookup_exact: Dict[Tuple[str, str], str] = {}
    lookup_normalized: Dict[Tuple[str, str], str] = {}
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT year_label, sheet_name, category FROM core.sheet_catalog")
        ).fetchall()
    for year_label, sheet_name, category in rows:
        if sheet_name is None:
            continue
        lookup_exact[(year_label, sheet_name)] = category
        lookup_normalized[(year_label, normalize_sheet_name(sheet_name))] = category
    return {"exact": lookup_exact, "normalized": lookup_normalized}


def lookup_category(
    catalog_lookup: Dict[str, Dict[Tuple[str, str], str]], year_label: str, sheet_name: str
) -> str | None:
    exact_key = (year_label, sheet_name)
    if exact_key in catalog_lookup["exact"]:
        return catalog_lookup["exact"][exact_key]
    normalized_key = (year_label, normalize_sheet_name(sheet_name))
    return catalog_lookup["normalized"].get(normalized_key)


def dataframe_for_sheet(
    workbook_path: Path, sheet_name: str, source_file: str
) -> pd.DataFrame:
    df = pd.read_excel(
        workbook_path,
        sheet_name=sheet_name,
        header=None,
        dtype=object,
    )
    if df.empty:
        return pd.DataFrame(
            columns=["sheet_name", "row_num", *COLUMNS, "source_file"]
        )

    max_columns = len(COLUMNS)
    if df.shape[1] < max_columns:
        df = df.reindex(columns=range(max_columns), fill_value=None)
    else:
        df = df.iloc[:, :max_columns]

    df.columns = COLUMNS
    df = df.where(pd.notnull(df), None)
    df.insert(0, "row_num", range(1, len(df) + 1))
    df.insert(0, "sheet_name", sheet_name)
    df["source_file"] = source_file
    return df


def truncate_inbox(conn) -> None:
    conn.execute(text(f"TRUNCATE TABLE {quoted_table(*INBOX_TABLE)};"))


def delete_existing_rows(
    conn, target_schema: str, target_table: str, sheet_name: str, source_file: str
) -> None:
    target_full = quoted_table(target_schema, target_table)
    conn.execute(
        text(
            f"""
DELETE FROM {target_full}
WHERE sheet_name = :sheet_name
  AND source_file = :source_file;
"""
        ),
        {"sheet_name": sheet_name, "source_file": source_file},
    )


def insert_from_inbox(conn, target_schema: str, target_table: str) -> int:
    target_full = quoted_table(target_schema, target_table)
    column_list = ", ".join(["sheet_name", "row_num", *COLUMNS, "source_file"])
    insert_sql = f"""
INSERT INTO {target_full} ({column_list})
SELECT {column_list}
FROM {quoted_table(*INBOX_TABLE)};
"""
    result = conn.execute(text(insert_sql))
    return result.rowcount if result.rowcount is not None else 0


def process_workbook(
    engine: Engine,
    year_label: str,
    target_table: str,
    workbook_path: Path,
    catalog_lookup: Dict[str, Dict[Tuple[str, str], str]],
) -> None:
    if not workbook_path.exists():
        raise FileNotFoundError(f"Workbook not found: {workbook_path}")

    target_schema, target_name = target_table.split(".")
    source_file = workbook_path.name
    excel_file = pd.ExcelFile(workbook_path)

    print(f"\nLoading workbook {source_file} ({year_label}) into {target_table}:")
    for sheet_name in excel_file.sheet_names:
        df = dataframe_for_sheet(workbook_path, sheet_name, source_file)
        row_count = len(df)
        category = lookup_category(catalog_lookup, year_label, sheet_name)
        category_display = category if category else "UNKNOWN"
        log_line = (
            f"  Sheet: {sheet_name} | Rows: {row_count} | Category: {category_display}"
        )
        if not category:
            normalized = normalize_sheet_name(sheet_name)
            log_line += f" (lookup attempted with normalized key '{normalized}')"
        print(log_line)

        with engine.begin() as conn:
            truncate_inbox(conn)
            if row_count:
                df.to_sql(
                    INBOX_TABLE[1],
                    con=conn,
                    schema=INBOX_TABLE[0],
                    if_exists="append",
                    index=False,
                )
            delete_existing_rows(conn, target_schema, target_name, sheet_name, source_file)
            inserted = insert_from_inbox(conn, target_schema, target_name)
            # Keep inbox clean even if no rows were inserted.
            truncate_inbox(conn)
        print(f"    -> Inserted {inserted} rows into {target_table}.")


def print_post_load_checks(engine: Engine) -> None:
    print("\nPost-load checks:")
    with engine.connect() as conn:
        for year_label, (target_table, _) in ALL_TABLE_WORKBOOKS.items():
            target_schema, target_name = target_table.split(".")
            target_full = quoted_table(target_schema, target_name)
            print(f"\n  Table {target_table} ({year_label}) sheet counts:")
            counts = conn.execute(
                text(
                    f"""
SELECT sheet_name, COUNT(*) AS row_count
FROM {target_full}
GROUP BY sheet_name
ORDER BY sheet_name;
"""
                )
            ).fetchall()
            if not counts:
                print("    (no rows)")
                continue
            for sheet_name, row_count in counts:
                print(f"    {sheet_name}: {row_count} rows")

            sample_sheet = counts[0][0]
            print(f"    Sample rows for sheet '{sample_sheet}':")
            sample_rows = conn.execute(
                text(
                    f"""
SELECT TOP 10 sheet_name, row_num, c01, c02, c03, c04, c05, source_file
FROM {target_full}
WHERE sheet_name = :sheet_name
ORDER BY row_num;
"""
                ),
                {"sheet_name": sample_sheet},
            ).fetchall()
            for row in sample_rows:
                print(f"      {row}")


def print_unmatched_sheets(engine: Engine) -> None:
    print("\nUnmatched sheet check (raw vs sheet_catalog):")
    query_template = """
WITH raw_sheets AS (
    SELECT DISTINCT :year_label AS year_label, sheet_name
    FROM {target_full}
)
SELECT r.year_label, r.sheet_name
FROM raw_sheets r
LEFT JOIN core.sheet_catalog c
    ON c.year_label = r.year_label AND c.sheet_name = r.sheet_name
WHERE c.sheet_id IS NULL
ORDER BY r.sheet_name;
"""
    with engine.connect() as conn:
        for year_label, (target_table, _) in ALL_TABLE_WORKBOOKS.items():
            target_schema, target_name = target_table.split(".")
            target_full = quoted_table(target_schema, target_name)
            print(f"\n  Year {year_label}:")
            rows = conn.execute(
                text(query_template.format(target_full=target_full)),
                {"year_label": year_label},
            ).fetchall()
            if not rows:
                print("    All sheets have catalog matches.")
            else:
                for row in rows:
                    print(f"    {row.sheet_name}")


def main() -> None:
    engine = create_engine_with_fallback()
    ensure_py_inbox(engine)
    catalog_lookup = load_catalog_lookup(engine)
    for year_label, (target_table, workbook_path) in ALL_TABLE_WORKBOOKS.items():
        process_workbook(engine, year_label, target_table, workbook_path, catalog_lookup)
    print_post_load_checks(engine)
    print_unmatched_sheets(engine)


if __name__ == "__main__":
    main()
