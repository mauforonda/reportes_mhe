#!/usr/bin/env python

import requests
import pandas as pd
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from pathlib import Path
from slugify import slugify
import os
from datetime import datetime, timezone

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
BASE_URL = "https://reportes.mhe.gob.bo"
session = requests.Session()
datadir = Path("data")


def list_datasets(session):
    datasets_json = session.get(f"{BASE_URL}/api/v1/dataset/", verify=False).json()
    datasets_df = pd.json_normalize(datasets_json["result"])
    return datasets_df[
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


def log_changes(new_df, datasets_path="datasets.csv", log_path="log.csv"):
    try:
        old_df = pd.read_csv(datasets_path)
    except FileNotFoundError:
        new_df.to_csv(datasets_path, index=False)
        return

    key = ["catalog", "database.database_name", "table_name"]
    old = old_df.set_index(key)
    new = new_df.set_index(key)
    ts = datetime.now(timezone.utc).isoformat()
    events = []

    added = new.index.difference(old.index)
    if len(added):
        for _, r in new.loc[added].reset_index().iterrows():
            events.append(
                {
                    "event_type": "added",
                    "catalog": r["catalog"],
                    "database": r["database.database_name"],
                    "table": r["table_name"],
                    "timestamp": ts,
                }
            )

    deleted = old.index.difference(new.index)
    if len(deleted):
        for _, r in old.loc[deleted].reset_index().iterrows():
            events.append(
                {
                    "event_type": "deleted",
                    "catalog": r["catalog"],
                    "database": r["database.database_name"],
                    "table": r["table_name"],
                    "timestamp": ts,
                }
            )

    common = new.index.intersection(old.index)
    if len(common):
        new_co = new["changed_on_utc"].reindex(common)
        old_co = old["changed_on_utc"].reindex(common)
        changed = new.loc[common][new_co != old_co]
        for _, r in changed.reset_index().iterrows():
            events.append(
                {
                    "event_type": "modified",
                    "catalog": r["catalog"],
                    "database": r["database.database_name"],
                    "table": r["table_name"],
                    "timestamp": ts,
                }
            )

    if events:
        events_df = pd.DataFrame(events)
        header = not Path(log_path).exists()
        events_df.to_csv(log_path, mode="a", header=header, index=False)

    new_df.to_csv(datasets_path, index=False)


print("Listing datasets ...")
datasets = list_datasets(session)
print("Updating log ...")
log_changes(datasets)
print("Fetching datasets ...")
for i, dataset in datasets.iterrows():
    filedir = datadir / dataset["catalog"]
    filename = (
        ".".join(
            [
                slugify(dataset[i], separator="_")
                for i in ["database.database_name", "table_name"]
            ]
        )
        + ".csv"
    )
    print(f"Fetching {filename}")
    df = fetch_dataset(session, dataset["id"])
    os.makedirs(filedir, exist_ok=True)
    df.to_csv(filedir / filename, index=False)
