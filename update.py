#!/usr/bin/env python

import requests
import pandas as pd
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from pathlib import Path
from slugify import slugify

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
BASE_URL = "https://reportes.mhe.gob.bo"
session = requests.Session()
datadir = Path("data")


def list_datasets(session):
    datasets_json = session.get(f"{BASE_URL}/api/v1/dataset/", verify=False).json()
    datasets_df = pd.json_normalize(datasets_json["result"])
    datasets_df[
        [
            "catalog",
            "changed_on_utc",
            "id",
            "kind",
            "schema",
            "sql",
            "table_name",
            "database.database_name",
            "database.id",
        ]
    ]
    return datasets_df


def fetch_dataset(session, dataset_id, chunk_size=10000, max_rows=None, verify=False):
    meta_response = session.get(
        f"{BASE_URL}/api/v1/dataset/{dataset_id}", verify=verify
    )
    meta_response.raise_for_status()
    dataset_info = meta_response.json()["result"]

    columns = [c["column_name"] for c in dataset_info.get("columns", [])]
    if not columns:
        raise ValueError(f"No columns found for dataset {dataset_id}")

    datasource = {"id": dataset_id, "type": "table"}

    all_rows = []
    offset = 0

    while True:
        if max_rows is not None:
            remaining = max_rows - len(all_rows)
            if remaining <= 0:
                break
            row_limit = min(chunk_size, remaining)
        else:
            row_limit = chunk_size

        payload = {
            "datasource": datasource,
            "queries": [
                {
                    "columns": columns,
                    "filters": [],
                    "metrics": [],
                    "row_limit": row_limit,
                    "row_offset": offset,
                    "time_range": "No filter",
                }
            ],
            "result_format": "json",
            "result_type": "full",
        }

        response = session.post(
            f"{BASE_URL}/api/v1/chart/data", json=payload, verify=verify
        )
        response.raise_for_status()

        result = response.json()["result"]
        if not result:
            break

        rows = result[0].get("data", [])
        if not rows:
            break

        all_rows.extend(rows)

        if len(rows) < row_limit:
            break

        offset += row_limit

    return pd.DataFrame(all_rows)


print("Listing datasets ...")
datasets = list_datasets(session)
datasets.to_csv("datasets.csv", index=False)
print("Fetching datasets ...")
for i, dataset in datasets.iterrows():
    filename = (
        "_".join(
            [
                slugify(dataset[i])
                for i in ["catalog", "database.database_name", "table_name"]
            ]
        )
        + ".csv"
    )
    print(f"Fetching {filename}")
    df = fetch_dataset(session, dataset["id"])
    df.to_csv(datadir / filename, index=False)
