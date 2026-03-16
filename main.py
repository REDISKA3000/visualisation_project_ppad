from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any

import pandas as pd

import cbr_extractor as cbr


def sanitize_identifier(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Unsafe SQL identifier: {name}")
    return name


def infer_sql_type(series: pd.Series) -> str:
    if pd.api.types.is_datetime64_any_dtype(series):
        return "TIMESTAMP"
    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"
    if pd.api.types.is_float_dtype(series):
        return "DOUBLE PRECISION"
    return "TEXT"


def create_table_sql(table: str, df: pd.DataFrame) -> str:
    cols = []
    for col in df.columns:
        sql_col = sanitize_identifier(col)
        cols.append(f"{sql_col} {infer_sql_type(df[col])}")
    cols_sql = ", ".join(cols)
    return f"CREATE TABLE IF NOT EXISTS {sanitize_identifier(table)} ({cols_sql});"


def insert_dataframe(connector: Any, table: str, df: pd.DataFrame, batch_size: int) -> None:
    if df.empty:
        print("[WARN] DataFrame is empty; nothing to insert.")
        return

    table = sanitize_identifier(table)
    columns = [sanitize_identifier(c) for c in df.columns]
    placeholders = ", ".join(["%s"] * len(columns))
    columns_sql = ", ".join(columns)
    query = f"INSERT INTO {table} ({columns_sql}) VALUES ({placeholders})"

    data = df.where(pd.notnull(df), None).values.tolist()
    connector.sql_query(query=query, insert_data=data, batch_size=batch_size)


def build_dataframes(out_dir: Path, run_discover: bool, skip_extract: bool) -> dict[str, Any]:
    products_dir = out_dir / "products"
    normalized_path = products_dir / "normalized_for_db.csv"

    if skip_extract:
        if not normalized_path.exists():
            raise FileNotFoundError(
                f"Missing {normalized_path}. Run extract/normalize first or disable --skip-extract."
            )
        normalized = pd.read_csv(normalized_path, parse_dates=["obs_date"], dayfirst=False)
    else:
        client = cbr.CBRClient()

        if run_discover:
            publications_df, datasets_df = cbr.discover_catalog(client, out_dir)
            shortlisted = cbr.filter_bank_datasets(datasets_df, out_dir)
            cbr.save_measures_and_years(client, shortlisted, out_dir)
            print(
                f"[OK] publications: {len(publications_df)}, datasets: {len(datasets_df)}, shortlisted: {len(shortlisted)}"
            )

        cbr.export_product_data(client, cbr.PRODUCT_CONFIG, products_dir)
        normalized = cbr.normalize_for_db(
            products_dir, normalized_path
        )

    product_frames: dict[str, pd.DataFrame] = {}
    if products_dir.exists():
        for csv_path in products_dir.glob("*.csv"):
            if csv_path.name == "normalized_for_db.csv":
                continue
            key = csv_path.stem
            product_frames[key] = pd.read_csv(csv_path)

    return {
        "normalized": normalized,
        "products": product_frames,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run CBR extraction and optionally load results into Postgres."
    )
    parser.add_argument("--out-dir", default="data/cbr")
    parser.add_argument("--run-discover", action="store_true")
    parser.add_argument("--load-db", action="store_true")
    parser.add_argument("--table", default="cbr_data")
    parser.add_argument("--team-suffix", default="_team_6")
    parser.add_argument("--batch-size", type=int, default=10000)
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="Do not call CBR API; use existing normalized_for_db.csv",
    )
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    result = build_dataframes(out_dir, run_discover=args.run_discover, skip_extract=args.skip_extract)
    df = result["normalized"]
    print(f"[OK] normalized rows: {len(df)}")

    if args.load_db:
        import connector as connector_mod

        table_name = args.table
        if args.team_suffix and not table_name.endswith(args.team_suffix):
            table_name = f"{table_name}{args.team_suffix}"

        insert_dataframe(connector_mod, table_name, df, args.batch_size)


if __name__ == "__main__":
    main()
