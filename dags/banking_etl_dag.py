from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess, logging, os, glob

default_args = {
    "owner"      : "data_team",
    "start_date" : datetime(2025, 1, 1),
    "retries"    : 0,              # ← غيّر من 1 لـ 0
}

def run_extract():
    from scripts.webhdfs_extract import extract
    logging.info("Uploaded batches: %s", extract())

def run_transform():
    logging.info("=== TRANSFORM ===")
    env = {
        **os.environ,
        "HADOOP_USER_NAME": "root",
        "JAVA_HOME"       : "/usr/lib/jvm/java-17-openjdk-amd64",
        "PATH"            : "/usr/lib/jvm/java-17-openjdk-amd64/bin:" + os.environ.get("PATH",""),
    }
    result = subprocess.run(
        ["python3", "/opt/airflow/spark/transform.py"],
        capture_output=True, text=True, env=env
    )
    logging.info(result.stdout)
    if result.returncode != 0:
        logging.error(result.stderr)
        raise RuntimeError("ETL stage failed; inspect private local logs")

def run_load():
    logging.info("=== LOAD ===")
    env = {
        **os.environ,
        "HADOOP_USER_NAME": "root",
        "JAVA_HOME"       : "/usr/lib/jvm/java-17-openjdk-amd64",
        "PATH"            : "/usr/lib/jvm/java-17-openjdk-amd64/bin:" + os.environ.get("PATH",""),
    }
    result = subprocess.run(
        ["python3", "/opt/airflow/spark/load.py"],
        capture_output=True, text=True, env=env
    )
    logging.info(result.stdout)
    if result.returncode != 0:
        logging.error(result.stderr)
        raise RuntimeError("ETL stage failed; inspect private local logs")

with DAG(
    dag_id      = "banking_etl_pipeline",
    default_args= default_args,
    schedule    = "0 0 * * *",
    catchup     = False,
    tags        = ["banking", "etl"],
) as dag:

    extract = PythonOperator(
        task_id         = "extract_to_hdfs",
        python_callable = run_extract,
    )
    transform = PythonOperator(
        task_id         = "spark_transform",
        python_callable = run_transform,
    )
    load = PythonOperator(
        task_id         = "load_to_snowflake",
        python_callable = run_load,
    )

    extract >> transform >> load
