# Banking Transactions ETL Pipeline

A learning integration of Airflow, WebHDFS, PySpark and Snowflake. Simulation writes batches,
the DAG uploads them, Spark builds dimensions/facts, and a bounded pandas transfer loads tables.

## Setup and security
Read [SECURITY.md](SECURITY.md) before starting. Docker Compose and a configured Snowflake
warehouse/schema are required for the full stack. Copy `.env.example` to ignored `.env.local`,
set credentials and local service secrets, then run:
```bash
docker compose --env-file .env.local up --build
python scripts/simulate_transactions.py
```
The tracked `.env` is deliberately a non-secret stub. Never put credentials there. Snowflake
role defaults to `BANKING_ETL`, which you must create/grant appropriately; it no longer assumes
ACCOUNTADMIN. The notebook reads credentials from its environment as well.

## Behavior and limits
HTTP upload stages are checked and sources move only after confirmed success. Date and type
dimension IDs are deterministic. Signed amounts are preserved; negative values are not silently
turned positive. The transform still rebuilds all staged tables and the warehouse loader replaces
their contents; this is **not an incremental production warehouse**. `MAX_WAREHOUSE_BATCH_ROWS`
bounds the pandas transfer (default 100000). Review account/merchant duplicate-attribute handling
before using changing dimensions. No fraud-model accuracy is claimed; suspicious flags are rules.

The public raw CSV is retained. Generated logs, exact checkpoint copies, bytecode, empty shell
artifacts and duplicate processed batches were removed from the working tree; history is retained.

## Offline checks
Python 3.12: `python -m pip install -r requirements-dev.txt`, then `python -m pytest -q`.
Tests check credential validation and failed/successful WebHDFS move semantics with a fake session.
They do not connect to Snowflake or prove Docker/Spark integration. Full deployment and least-privilege
grants remain owner-environment verification steps. No private credential was used in these changes.
