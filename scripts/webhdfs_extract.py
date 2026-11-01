"""Move a batch only after both HTTP stages confirm a successful upload."""
from pathlib import Path
from datetime import date
import csv
import os
import requests

def validate_batch(path):
    with Path(path).open(newline="", encoding="utf-8-sig") as source:
        reader = csv.DictReader(source)
        required = {"TransactionID", "AccountID", "TransactionAmount", "TransactionDate"}
        if not required <= set(reader.fieldnames or []): raise ValueError("Batch schema is incomplete")
        rows = list(reader)
        if not rows or any(not (row.get(key) or "").strip() for row in rows for key in required):
            raise ValueError("Batch has no rows or missing required values")
    return len(rows)

def extract(landing="/opt/airflow/data/landing", processed="/opt/airflow/data/processed", session=None):
    session = requests if session is None else session
    target = Path(processed); target.mkdir(parents=True, exist_ok=True)
    base = os.getenv("WEBHDFS_URL", "http://hadoop-namenode:9870/webhdfs/v1")
    folder = f"/banking/raw/date={date.today().isoformat()}"
    response = session.put(base + folder, params={"op": "MKDIRS", "user.name": "root"}, timeout=30)
    response.raise_for_status()
    count = 0
    for path in sorted(Path(landing).glob("*.csv")):
        validate_batch(path)
        if (target/path.name).exists(): raise ValueError("Processed filename already exists; refusing to overwrite")
        response = session.put(base + folder + "/" + path.name,
            params={"op": "CREATE", "user.name": "root", "overwrite": "true"}, allow_redirects=False, timeout=30)
        response.raise_for_status()
        location = response.headers.get("Location")
        if response.status_code != 307 or not location: raise RuntimeError("HDFS did not provide an upload redirect")
        with path.open("rb") as source:
            uploaded = session.put(location, data=source, headers={"Content-Type": "application/octet-stream"}, timeout=60)
            uploaded.raise_for_status()
            if uploaded.status_code != 201:
                raise RuntimeError("HDFS upload was not confirmed as created")
        path.rename(target/path.name); count += 1
    return count
