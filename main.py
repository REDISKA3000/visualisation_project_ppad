from __future__ import annotations

import argparse
import re
import os
import io
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg2

import cbr_extractor as cbr


def sanitize_identifier(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Unsafe SQL identifier: {name}")
    return name


def sanitize_table_name(name: str) -> str:
    parts = name.split(".")
    if not parts:
        raise ValueError(f"Unsafe SQL table name: {name}")
    return ".".join(sanitize_identifier(p) for p in parts)


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


def insert_dataframe(
    conn: psycopg2.extensions.connection,
    cursor: psycopg2.extensions.cursor,
    table: str,
    df: pd.DataFrame,
    batch_size: int,
    conflict_cols: list[str] | None = None,
    use_copy: bool = True,
) -> None:
    if df.empty:
        print("[WARN] DataFrame is empty; nothing to insert.")
        return

    table = sanitize_table_name(table)
    columns = [sanitize_identifier(c) for c in df.columns]
    placeholders = ", ".join(["%s"] * len(columns))
    columns_sql = ", ".join(columns)
    if use_copy and conflict_cols is None:
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False, sep="\t", na_rep="\\N")
        buffer.seek(0)
        copy_sql = (
            f"COPY {table} ({columns_sql}) FROM STDIN "
            "WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\\\N')"
        )
        cursor.copy_expert(copy_sql, buffer)
        conn.commit()
        return

    query = f"INSERT INTO {table} ({columns_sql}) VALUES ({placeholders})"
    if conflict_cols is not None:
        conflict_cols_sql = ", ".join(sanitize_identifier(c) for c in conflict_cols)
        query = f"{query} ON CONFLICT ({conflict_cols_sql}) DO NOTHING"

    data = df.where(pd.notnull(df), None).values.tolist()
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        cursor.executemany(query, batch)
        conn.commit()
        print(f"Батч с {i} по {i + len(batch) - 1} успешно вставлен")


def build_dataframes(out_dir: Path, run_discover: bool, skip_extract: bool) -> dict[str, Any]:
    products_dir = out_dir / "products"
    normalized_path = products_dir / "normalized_for_db.csv"

    if skip_extract:
        if not products_dir.exists():
            raise FileNotFoundError(
                f"Missing {products_dir}. Run extract first or disable --skip-extract."
            )
        normalized = None
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
        normalized = cbr.normalize_for_db(products_dir, normalized_path)

    product_frames: dict[str, pd.DataFrame] = {}
    if products_dir.exists():
        for csv_path in products_dir.glob("*.csv"):
            if csv_path.name == "normalized_for_db.csv":
                continue
            key = csv_path.stem
            product_frames[key] = pd.read_csv(csv_path)

    return {"normalized": normalized, "products": product_frames}


def build_table_name(base: str, product_key: str, team_suffix: str) -> str:
    table = f"{base}{product_key}"
    if team_suffix and not table.endswith(team_suffix):
        table = f"{table}{team_suffix}"
    return table


def append_partition_suffix(table_name: str, year: int) -> str:
    if "." in table_name:
        schema, table = table_name.split(".", 1)
        return f"{schema}.{table}_{year}"
    return f"{table_name}_{year}"


def prepare_product_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"colId": "col_id", "rowId": "row_id"})
    ordered_cols = [
        "col_id",
        "element_id",
        "measure_id",
        "unit_id",
        "obs_val",
        "row_id",
        "dt",
        "periodicity",
        "date",
        "digits",
        "element_name",
        "unit_name",
        "product_key",
        "product_description",
        "measure_name",
    ]
    present = [c for c in ordered_cols if c in df.columns]
    return df[present]


def split_by_year(df: pd.DataFrame, date_col: str = "date") -> dict[int | None, pd.DataFrame]:
    if date_col not in df.columns:
        return {None: df}

    dates = pd.to_datetime(df[date_col], errors="coerce")
    df = df.copy()
    df[date_col] = dates.dt.date
    missing = dates.isna()
    if missing.any():
        print(f"[WARN] {missing.sum()} rows have invalid {date_col}; they will be skipped.")
        df = df.loc[~missing].copy()
        dates = dates.loc[~missing]

    if df.empty:
        return {}

    years = dates.dt.year
    df["_year"] = years.values
    groups: dict[int, pd.DataFrame] = {}
    for year, sub in df.groupby("_year", sort=True):
        groups[int(year)] = sub.drop(columns=["_year"])
    return groups


def load_spare_frames(out_dir: Path) -> list[tuple[str, pd.DataFrame, list[str]]]:
    base = out_dir
    spare_jobs: list[tuple[str, pd.DataFrame, list[str]]] = []

    publications = pd.read_csv(base / "publications.csv").rename(columns={"NoActive": "no_active"})
    spare_jobs.append(("hr_final_projects.team_6_publications", publications, ["id"]))

    datasets_catalog = pd.read_csv(base / "datasets_catalog.csv")
    spare_jobs.append(
        ("hr_final_projects.team_6_datasets_catalog", datasets_catalog, ["dataset_id", "publication_id"])
    )

    datasets_short = pd.read_csv(base / "datasets_bank_shortlist.csv")
    spare_jobs.append(
        ("hr_final_projects.team_6_datasets_bank_shortlist", datasets_short, ["dataset_id", "publication_id"])
    )

    measures = pd.read_csv(base / "measures.csv")
    spare_jobs.append(("hr_final_projects.team_6_measures", measures, ["dataset_id", "measure_id"]))

    years = pd.read_csv(base / "years.csv")
    spare_jobs.append(("hr_final_projects.team_6_years", years, ["dataset_id", "measure_id"]))

    return spare_jobs


def load_env_file() -> None:
    env_candidates = [
        Path(__file__).with_name("_env"),
        Path(__file__).with_name(".env"),
        Path.cwd() / "_env",
        Path.cwd() / ".env",
    ]
    try:
        from dotenv import load_dotenv
    except Exception:
        return

    for path in env_candidates:
        if path.exists():
            load_dotenv(path)
            return


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run CBR extraction and optionally load results into Postgres."
    )
    parser.add_argument("--out-dir", default="data/cbr")
    parser.add_argument("--run-discover", action="store_true")
    parser.add_argument("--load-db", action="store_true")
    parser.add_argument(
        "--table-prefix",
        default="hr_final_projects.team_6_",
        help="Prefix for per-product tables (e.g. hr_final_projects.team_6_mortgage).",
    )
    parser.add_argument("--team-suffix", default="")
    parser.add_argument("--batch-size", type=int, default=10000)
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="Do not call CBR API; use existing normalized_for_db.csv",
    )
    parser.add_argument(
        "--skip-spare",
        action="store_true",
        help="Do not load spare/reference tables.",
    )
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    result = build_dataframes(out_dir, run_discover=args.run_discover, skip_extract=args.skip_extract)
    products = result["products"]
    if result["normalized"] is not None:
        print(f"[OK] normalized rows: {len(result['normalized'])}")
    print(f"[OK] product tables: {len(products)}")

    if args.load_db:
        load_env_file()
        import connector as connector_mod

        jobs: list[tuple[str, pd.DataFrame, list[str] | None]] = []

        if not args.skip_spare:
            for table_name, df, conflict_cols in load_spare_frames(out_dir):
                jobs.append((table_name, df, conflict_cols))

        for product_key, df in products.items():
            table_name = build_table_name(args.table_prefix, product_key, args.team_suffix)
            prepared = prepare_product_df(df)
            yearly = split_by_year(prepared, date_col="date")
            if yearly:
                for year, sub in yearly.items():
                    if 2019 <= year <= 2026:
                        part_table = append_partition_suffix(table_name, year)
                        jobs.append((part_table, sub, None))
                    else:
                        jobs.append((table_name, sub, None))
            else:
                jobs.append((table_name, prepared, None))

        total = len(jobs)
        with psycopg2.connect(**connector_mod.pg_creds) as conn:
            with conn.cursor() as cursor:
                for idx, (table_name, df, conflict_cols) in enumerate(jobs, start=1):
                    print(f"[{idx}/{total}] Loading {table_name} ({len(df)} rows)")
                    use_copy = conflict_cols is None
                    insert_dataframe(
                        conn,
                        cursor,
                        table_name,
                        df,
                        args.batch_size,
                        conflict_cols,
                        use_copy=use_copy,
                    )
                    print(f"[{idx}/{total}] Done {table_name}")


if __name__ == "__main__":
    main()
