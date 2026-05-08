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
    logging.info("=== EXTRACT ===")
    import requests
    from datetime import datetime as dt
    
    landing_files = glob.glob("/opt/airflow/data/landing/*.csv")
    logging.info(f"Files: {len(landing_files)}")
    if not landing_files:
        logging.warning("No files in landing zone")
        return

    today     = dt.now().strftime("%Y-%m-%d")
    hdfs_base = "http://hadoop-namenode:9870/webhdfs/v1"
    hdfs_path = f"/banking/raw/date={today}"
    user      = "root"

    # عمل الـ directory
    requests.put(
        f"{hdfs_base}{hdfs_path}?op=MKDIRS&user.name={user}"
    )
    logging.info(f"HDFS dir ready: {hdfs_path}")

    for f in landing_files:
        fname = os.path.basename(f)

        # الخطوة 1: ابعت الـ CREATE request وخد الـ redirect URL
        r = requests.put(
            f"{hdfs_base}{hdfs_path}/{fname}?op=CREATE&user.name={user}&overwrite=true",
            allow_redirects=False
        )
        redirect_url = r.headers.get("Location")

        # الخطوة 2: ارفع الـ file على الـ redirect URL
        with open(f, "rb") as file_data:
            requests.put(redirect_url, data=file_data,
                        headers={"Content-Type": "application/octet-stream"})

        os.rename(f, f"/opt/airflow/data/processed/{fname}")
        logging.info(f"✓ {fname}")

    logging.info("Extract complete")

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
        raise RuntimeError(result.stderr)

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
        raise RuntimeError(result.stderr)

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