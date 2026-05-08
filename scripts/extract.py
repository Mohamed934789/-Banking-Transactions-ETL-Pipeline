import subprocess, os, glob, logging
from datetime import datetime

LANDING_ZONE  = '/opt/airflow/data/landing'
HDFS_RAW_PATH = 'hdfs://hadoop-namenode:9000/banking/raw'
PROCESSED_DIR = '/opt/airflow/data/processed'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [EXTRACT] %(message)s')

def run_hdfs_cmd(args):
    result = subprocess.run(['hdfs', 'dfs'] + args,
                            capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f'HDFS error: {result.stderr}')
    return result.stdout

def extract():
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    today       = datetime.now().strftime('%Y-%m-%d')
    hdfs_target = f'{HDFS_RAW_PATH}/date={today}'
    logging.info(f'Landing zone: {LANDING_ZONE}')
    batch_files = glob.glob(f'{LANDING_ZONE}/*.csv')
    logging.info(f'Files found: {len(batch_files)}')
    if not batch_files:
        logging.warning('No files in landing zone.')
        return
    run_hdfs_cmd(['-mkdir', '-p', hdfs_target])
    for f in batch_files:
        filename = os.path.basename(f)
        logging.info(f'Uploading {filename}...')
        run_hdfs_cmd(['-put', '-f', f, f'{hdfs_target}/{filename}'])
        os.rename(f, f'{PROCESSED_DIR}/{filename}')
        logging.info(f'Done: {filename}')
    logging.info(f'Extract complete — {len(batch_files)} files → HDFS')

if __name__ == '__main__':
    extract()