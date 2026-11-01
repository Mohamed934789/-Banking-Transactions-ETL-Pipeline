"""Read-only credential configuration. Never include values in errors/logs."""
import os

def snowflake_options(environment=None):
    env = os.environ if environment is None else environment
    fields = {"account": "SNOWFLAKE_ACCOUNT", "user": "SNOWFLAKE_USER", "password": "SNOWFLAKE_PASSWORD"}
    missing = [name for name in fields.values() if not env.get(name, "").strip()]
    if missing: raise ValueError("Missing Snowflake environment variables: " + ", ".join(missing))
    return {**{key: env[name].strip() for key, name in fields.items()},
        "database": env.get("SNOWFLAKE_DB", "BANKING_DWH"), "schema": env.get("SNOWFLAKE_SCHEMA", "STAR_SCHEMA"),
        "warehouse": env.get("SNOWFLAKE_WH", "COMPUTE_WH"), "role": env.get("SNOWFLAKE_ROLE", "BANKING_ETL")}
