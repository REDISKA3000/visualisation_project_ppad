from __future__ import annotations

import argparse
import ast
import re
import types
from pathlib import Path
from typing import Any

import pandas as pd

import cbr_extractor as cbr


def load_connector_module(connector_path: Path) -> types.ModuleType:
    """
    Safely load sql_query and pg_creds from connector.py without executing
    any top-level example code in that file.
    """
    source = connector_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(connector_path))

    new_body: list[ast.AST] = []
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            new_body.append(node)
        elif isinstance(node, ast.Assign):
            if any(
                isinstance(t, ast.Name) and t.id == "pg_creds"
                for t in node.targets
            ):
                new_body.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name == "sql_query":
            new_body.append(node)

    module = types.ModuleType("connector_safe")
    safe_tree = ast.Module(body=new_body, type_ignores=[])
    code = compile(safe_tree, str(connector_path), "exec")
    exec(code, module.__dict__)
    return module


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


def insert_dataframe(connector: types.ModuleType, table: str, df: pd.DataFrame, batch_size: int) -> None:
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


def build_dataframes(out_dir: Path, run_discover: bool) -> dict[str, Any]:
    client = cbr.CBRClient()

    if run_discover:
        publications_df, datasets_df = cbr.discover_catalog(client, out_dir)
        shortlisted = cbr.filter_bank_datasets(datasets_df, out_dir)
        cbr.save_measures_and_years(client, shortlisted, out_dir)
        print(
            f"[OK] publications: {len(publications_df)}, datasets: {len(datasets_df)}, shortlisted: {len(shortlisted)}"
        )

    cbr.export_product_data(client, cbr.PRODUCT_CONFIG, out_dir / "products")
    normalized = cbr.normalize_for_db(
        out_dir / "products", out_dir / "products" / "normalized_for_db.csv"
    )

    product_frames: dict[str, pd.DataFrame] = {}
    for csv_path in (out_dir / "products").glob("*.csv"):
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
    parser.add_argument("--batch-size", type=int, default=10000)
    parser.add_argument("--create-table", action="store_true")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    result = build_dataframes(out_dir, run_discover=args.run_discover)
    df = result["normalized"]
    print(f"[OK] normalized rows: {len(df)}")

    if args.load_db:
        connector_path = Path(__file__).with_name("connector.py")
        connector = load_connector_module(connector_path)
        if args.create_table:
            create_sql = create_table_sql(args.table, df)
            connector.sql_query(query=create_sql, commit=True)
        insert_dataframe(connector, args.table, df, args.batch_size)


if __name__ == "__main__":
    main()
