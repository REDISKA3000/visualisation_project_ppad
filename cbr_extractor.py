from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from requests.exceptions import RequestException, Timeout

BASE_URL = "https://www.cbr.ru/dataservice"
TIMEOUT = 15
SLEEP_SEC = 0.15


@dataclass
class DatasetRecord:
    publication_id: int
    publication_name: str
    dataset_id: int
    dataset_name: str
    dataset_type: int | None
    publication_no_active: bool | None = None


class CBRClient:
    def __init__(self, base_url: str = BASE_URL, timeout: int = TIMEOUT):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.timeout = timeout

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        try:
            response = self.session.get(
                f"{self.base_url}/{path.lstrip('/')}",
                params=params,
                timeout=(self.timeout, self.timeout),
            )
            response.raise_for_status()
            return response.json()
        except Timeout as exc:
            raise RuntimeError(f"Request timed out after {self.timeout}s: {path}") from exc
        except RequestException as exc:
            raise RuntimeError(f"Request failed for {path}: {exc}") from exc

    def publications(self) -> list[dict[str, Any]]:
        return self._get("publications")

    def datasets(self, publication_id: int) -> list[dict[str, Any]]:
        return self._get("datasets", params={"publicationId": publication_id})

    def measures(self, dataset_id: int) -> list[dict[str, Any]]:
        payload = self._get("measures", params={"datasetId": dataset_id})
        if isinstance(payload, dict):
            return payload.get("measure", [])
        return payload

    def years(self, dataset_id: int, measure_id: int = -1) -> list[dict[str, Any]]:
        return self._get("years", params={"datasetId": dataset_id, "measureId": measure_id})

    def data(
        self,
        publication_id: int,
        dataset_id: int,
        y1: int,
        y2: int,
        measure_id: int = -1,
    ) -> Any:
        return self._get(
            "data",
            params={
                "publicationId": publication_id,
                "datasetId": dataset_id,
                "measureId": measure_id,
                "y1": y1,
                "y2": y2,
            },
        )


def slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-zа-яё0-9]+", "_", value, flags=re.IGNORECASE)
    return value.strip("_")[:120] or "dataset"


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def discover_catalog(client: CBRClient, out_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    ensure_dir(out_dir)

    publications = client.publications()
    publications_df = pd.json_normalize(publications)
    publications_df.to_csv(out_dir / "publications.csv", index=False)
    (out_dir / "publications.json").write_text(
        json.dumps(publications, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    dataset_rows: list[DatasetRecord] = []
    for publication in publications:
        publication_id = int(publication["id"])
        publication_name = publication.get("category_name", "")
        no_active = publication.get("NoActive")

        try:
            datasets = client.datasets(publication_id)
        except Exception as exc:
            print(
                f"[WARN] publicationId={publication_id} datasets error: {exc}")
            continue

        for ds in datasets:
            dataset_rows.append(
                DatasetRecord(
                    publication_id=publication_id,
                    publication_name=publication_name,
                    dataset_id=int(ds["id"]),
                    dataset_name=ds.get("name", ""),
                    dataset_type=ds.get("type"),
                    publication_no_active=no_active,
                )
            )
        time.sleep(SLEEP_SEC)

    datasets_df = pd.DataFrame([asdict(r) for r in dataset_rows])
    datasets_df.to_csv(out_dir / "datasets_catalog.csv", index=False)
    return publications_df, datasets_df


def filter_bank_datasets(datasets_df: pd.DataFrame, out_dir: Path) -> pd.DataFrame:
    bank_patterns = [
        "ипот",
        "кредит",
        "вклад",
        "депозит",
        "средства",
        "юр",
        "ип",
        "физ",
        "банк",
        "просроч",
        "субъект",
        "регион",
    ]
    regex = "|".join(bank_patterns)
    mask = (
        datasets_df["publication_name"].fillna(
            "").str.contains(regex, case=False, regex=True)
        | datasets_df["dataset_name"].fillna("").str.contains(regex, case=False, regex=True)
    )
    shortlisted = datasets_df.loc[mask].copy().sort_values(
        ["publication_name", "dataset_name"]
    )
    shortlisted.to_csv(out_dir / "datasets_bank_shortlist.csv", index=False)
    return shortlisted


def save_measures_and_years(client: CBRClient, datasets_df: pd.DataFrame, out_dir: Path) -> None:
    ensure_dir(out_dir)
    measures_rows: list[dict[str, Any]] = []
    years_rows: list[dict[str, Any]] = []

    for _, row in datasets_df.iterrows():
        dataset_id = int(row["dataset_id"])
        dataset_type = row.get("dataset_type")

        measure_ids = [-1]
        if pd.notna(dataset_type) and int(dataset_type) == 1:
            try:
                measures = client.measures(dataset_id)
            except Exception as exc:
                print(f"[WARN] datasetId={dataset_id} measures error: {exc}")
                measures = []

            for m in measures:
                measures_rows.append(
                    {
                        "dataset_id": dataset_id,
                        "measure_id": m.get("id"),
                        "measure_name": m.get("name"),
                    }
                )
            measure_ids = [int(m.get("id"))
                           for m in measures if m.get("id") is not None] or [-1]

        for measure_id in measure_ids:
            try:
                years = client.years(dataset_id=dataset_id,
                                     measure_id=measure_id)
            except Exception as exc:
                print(
                    f"[WARN] datasetId={dataset_id} measureId={measure_id} years error: {exc}")
                continue

            for y in years:
                years_rows.append(
                    {
                        "dataset_id": dataset_id,
                        "measure_id": measure_id,
                        "from_year": y.get("FromYear"),
                        "to_year": y.get("ToYear"),
                    }
                )
        time.sleep(SLEEP_SEC)

    pd.DataFrame(measures_rows).drop_duplicates().to_csv(
        out_dir / "measures.csv", index=False)
    pd.DataFrame(years_rows).drop_duplicates().to_csv(
        out_dir / "years.csv", index=False)


def flatten_data_payload(payload: Any) -> pd.DataFrame:
    if isinstance(payload, list):
        return pd.json_normalize(payload)
    if isinstance(payload, dict):
        row_data = payload.get("RowData") or payload.get("rowData")
        links = payload.get("Links") or payload.get("links")

        if isinstance(row_data, list):
            data_df = pd.json_normalize(row_data)
        else:
            data_df = pd.json_normalize(payload)

        if isinstance(links, list) and not data_df.empty:
            links_df = pd.json_normalize(links)
            join_keys = [
                col
                for col in [
                    "indicator_id",
                    "measure1_id",
                    "measure2_id",
                    "unit_id",
                ]
                if col in data_df.columns and col in links_df.columns
            ]
            if join_keys:
                data_df = data_df.merge(
                    links_df, how="left", on=join_keys, suffixes=("", "_link"))
        return data_df
    return pd.DataFrame()


PRODUCT_CONFIG = [
    {
        "product_key": "mortgage",
        "description": "Ипотека (региональный срез)",
        "publication_id": 15,
        "dataset_id": 34,
        "measure_id": -1,
        "y1": 2019,
        "y2": 2025,
    },
    {
        "product_key": "retail_loans",
        "description": "Кредиты физлицам (региональный срез, рубли)",
        "publication_id": 15,
        "dataset_id": 32,
        "measure_id": -1,
        "y1": 2019,
        "y2": 2025,
    },
    {
        "product_key": "corp_sme_loans",
        "description": "Кредиты юрлицам и ИП (МСП, региональный срез, рубли)",
        "publication_id": 15,
        "dataset_id": 31,
        "measure_id": -1,
        "y1": 2019,
        "y2": 2025,
    },
    {
        "product_key": "deposits",
        "description": "Вклады физлиц (региональный срез, рубли)",
        "publication_id": 19,
        "dataset_id": 39,
        "measure_id": -1,
        "y1": 2019,
        "y2": 2025,
    },
]


def export_product_data(client: CBRClient, config: list[dict[str, Any]], out_dir: Path) -> None:
    ensure_dir(out_dir)
    raw_dir = out_dir / "raw_json"
    ensure_dir(raw_dir)

    for item in config:
        if not item.get("publication_id") or not item.get("dataset_id"):
            print(
                f"[SKIP] {item['product_key']}: publication_id/dataset_id пока не заполнены")
            continue

        dataset_id = int(item["dataset_id"])
        publication_id = int(item["publication_id"])
        base_measure_id = int(item.get("measure_id", -1))

        measures: list[dict[str, Any]] = []
        if base_measure_id == -1:
            try:
                measures = client.measures(dataset_id)
            except Exception as exc:
                print(f"[WARN] datasetId={dataset_id} measures error: {exc}")
                measures = []

        if measures:
            measure_list = [
                (int(m.get("id")), m.get("name"))
                for m in measures
                if m.get("id") is not None
            ]
        else:
            measure_list = [(base_measure_id, None)]

        payloads: list[dict[str, Any]] = []
        frames: list[pd.DataFrame] = []

        for measure_id, measure_name in measure_list:
            payload = client.data(
                publication_id=publication_id,
                dataset_id=dataset_id,
                measure_id=measure_id,
                y1=int(item["y1"]),
                y2=int(item["y2"]),
            )
            payloads.append(
                {
                    "measure_id": measure_id,
                    "measure_name": measure_name,
                    "payload": payload,
                }
            )

            df = flatten_data_payload(payload)
            if df.empty:
                time.sleep(SLEEP_SEC)
                continue
            df["product_key"] = item["product_key"]
            df["product_description"] = item["description"]
            df["measure_id"] = measure_id
            if measure_name is not None:
                df["measure_name"] = measure_name
            frames.append(df)
            time.sleep(SLEEP_SEC)

        raw_path = raw_dir / f"{item['product_key']}.json"
        raw_path.write_text(
            json.dumps(payloads, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        out_file = out_dir / f"{item['product_key']}.csv"
        if frames:
            out_df = pd.concat(frames, ignore_index=True, sort=False)
            out_df.to_csv(out_file, index=False)
            print(f"[OK] Saved {item['product_key']} -> {out_file} ({len(out_df)} rows)")
        else:
            out_file.write_text("", encoding="utf-8")
            print(f"[WARN] No data for {item['product_key']} (dataset_id={dataset_id})")


NORMALIZED_RENAME_MAP = {
    "date": "obs_date",
    "periodicity": "periodicity",
    "obs_val": "value",
    "indicator_name": "metric_name",
    "measure1_name": "dim_1_name",
    "measure2_name": "dim_2_name",
    "un_name": "unit_name",
}


def normalize_for_db(product_csv_dir: Path, out_path: Path) -> pd.DataFrame:
    csv_files = [p for p in product_csv_dir.glob(
        "*.csv") if p.name != out_path.name]
    frames: list[pd.DataFrame] = []

    for file_path in csv_files:
        df = pd.read_csv(file_path)
        df.columns = [c.strip() for c in df.columns]
        df = df.rename(
            columns={k: v for k, v in NORMALIZED_RENAME_MAP.items() if k in df.columns})

        for col in ["obs_date"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        if "value" in df.columns:
            df["value"] = pd.to_numeric(df["value"], errors="coerce")

        frames.append(df)

    if not frames:
        result = pd.DataFrame()
    else:
        result = pd.concat(frames, ignore_index=True, sort=False)

    preferred_cols = [
        "product_key",
        "product_description",
        "obs_date",
        "periodicity",
        "metric_name",
        "dim_1_name",
        "dim_2_name",
        "value",
        "unit_name",
    ]
    ordered = [c for c in preferred_cols if c in result.columns] + \
        [c for c in result.columns if c not in preferred_cols]
    result = result[ordered] if not result.empty else result
    result.to_csv(out_path, index=False)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Explore and extract CBR datasets for bank expansion analysis")
    parser.add_argument("command", choices=[
                        "discover", "extract", "normalize", "all"])
    parser.add_argument("--out-dir", default="data/cbr")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    client = CBRClient()

    if args.command in {"discover", "all"}:
        publications_df, datasets_df = discover_catalog(client, out_dir)
        shortlisted = filter_bank_datasets(datasets_df, out_dir)
        save_measures_and_years(client, shortlisted, out_dir)
        print(
            f"[OK] publications: {len(publications_df)}, datasets: {len(datasets_df)}, shortlisted: {len(shortlisted)}")

    if args.command in {"extract", "all"}:
        export_product_data(client, PRODUCT_CONFIG, out_dir / "products")

    if args.command in {"normalize", "all"}:
        normalized = normalize_for_db(
            out_dir / "products", out_dir / "products" / "normalized_for_db.csv")
        print(f"[OK] normalized rows: {len(normalized)}")


if __name__ == "__main__":
    main()
