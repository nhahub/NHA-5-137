"""
Apache Airflow DAG: Smart Warehouse Unified End-to-End Lakehouse Pipeline.

Complete Architecture & Workflow:
================================================================================
1. [Raw Generation]       : Dynamically generates all 21 raw datasets with large volume ('full') & injected defects.
2. [Data Quality Checks]  : Comprehensive pre-ingestion validation (completeness, file sizes, schema sampling).
3. [Partitioned S3 Upload]: Parallel partitioned upload to AWS S3 (configured via .env):
                            - Master / Reference : raw/<table_name>/snapshot_date=YYYY-MM-DD/<file>.csv
                            - Transactional      : raw/<table_name>/year=YYYY/month=MM/day=DD/<file>.csv.gz
                            - Ledger Streams     : raw/inventory_transactions/year=YYYY/month=MM/day=DD/<file>.jsonl.gz
                            - Weather Streams    : raw/weather_daily/year=YYYY/month=MM/<file>.json
4. [Local Cleanup]        : Automatically purges all local raw files from disk after successful S3 ingestion.
5. [Glue Pre-Flight]      : Verifies AWS credentials, region, and readiness of all 6 AWS Glue Jobs.
6. [Bronze Layer Parallel]: Concurrently executes all 3 Bronze AWS Glue Jobs:
                            - Bronze_01_reference   (Reference/Master tables -> Bronze Iceberg)
                            - Bronze_02_transactions (Sales Orders, POs, Receipts -> Bronze Iceberg)
                            - Bronze_03_inventory_ops (Inventory Ledger, Daily Snapshots, Weather -> Bronze Iceberg)
7. [Bronze Sync Barrier]  : Synchronization point (all_success). Guarantees that all Bronze jobs succeed
                            completely before initiating the Silver stage.
8. [Silver Layer Parallel]: Concurrently executes all 3 Silver AWS Glue Jobs:
                            - Silver_01_master_commercial (Cleansing, deduplication, surrogate keys -> Silver Iceberg)
                            - Silver_02_transactions      (Fact tables, headers/lines rollups, order merge -> Silver Iceberg)
                            - Silver_03_inventory_ops     (Ledger, snapshots roll-forward, temperature fix -> Silver Iceberg)
9. [Pipeline Completion]  : Final validation marker confirming successful end-to-end lakehouse run.

Core Principles:
================================================================================
- ZERO Hardcoding: S3 buckets, AWS regions, and credentials dynamically sourced from .env on every run.
- True Parallelism: 3 S3 upload streams run concurrently; 3 Bronze jobs run concurrently; 3 Silver jobs run concurrently.
- Strict Integrity: Silver jobs will NEVER run if any Bronze job fails or encounters an unhandled exception.
"""

import os
import sys
import json
import gzip
import shutil
from datetime import datetime, timedelta

from airflow import DAG

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:
    from airflow.operators.empty import EmptyOperator

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator

from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.utils.trigger_rule import TriggerRule

import boto3


# Ensure project root is available in sys.path
for candidate_dir in ["/opt/airflow", os.path.dirname(os.path.dirname(os.path.abspath(__file__)))]:
    if candidate_dir not in sys.path and os.path.exists(candidate_dir):
        sys.path.append(candidate_dir)


# ==============================================================================
# Dynamic Environment Configuration (.env loader)
# ==============================================================================
def load_env_variables() -> dict:
    """
    Dynamically loads and refreshes environment variables directly from the .env file.
    Ensures zero hardcoding and immediate reactivity whenever .env is modified.
    """
    candidate_paths = [
        "/opt/airflow/.env",
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"),
        os.path.join(os.getcwd(), ".env"),
    ]
    loaded = {}
    for env_path in candidate_paths:
        if os.path.exists(env_path) and os.path.isfile(env_path):
            try:
                with open(env_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#") or "=" not in line:
                            continue
                        k, v = line.split("=", 1)
                        k = k.strip()
                        v = v.strip().strip("'\"")
                        os.environ[k] = v
                        loaded[k] = v
                break
            except Exception as e:
                print(f"[ENV] Warning: Failed to parse {env_path}: {e}")
    return loaded


# Initial load at parse time
load_env_variables()

AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
AWS_CONN_ID = os.environ.get("AIRFLOW_AWS_CONN_ID", None)

MASTER_TABLES = [
    "product_categories", "products", "warehouses", "suppliers",
    "supplier_products", "customers", "product_prices", "product_costs",
    "promotions", "calendar_events"
]

TRANSACTIONAL_TABLES = [
    "sales_orders", "sales_order_lines", "sales_returns",
    "purchase_orders", "purchase_order_lines", "goods_receipt_lines",
    "product_batches", "inventory_snapshots", "stockout_events"
]

GLUE_JOBS_BRONZE = [
    "Bronze_01_reference",
    "Bronze_02_transactions",
    "Bronze_03_inventory_ops",
]

GLUE_JOBS_SILVER = [
    "Silver_01_master_commercial",
    "Silver_02_transactions",
    "Silver_03_inventory_ops",
]


# ==============================================================================
# Helper Functions: Paths & AWS Clients
# ==============================================================================
def _get_raw_data_path() -> str:
    """Gets local raw data directory dynamically from .env."""
    load_env_variables()
    if os.path.exists("/opt/airflow"):
        return os.environ.get("AIRFLOW_DATA_DIR", "/opt/airflow/data/raw")
    return os.environ.get(
        "RAW_DATA_PATH",
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw")
    )


def _get_s3_bucket() -> str:
    """Gets target AWS S3 raw bucket name dynamically from .env."""
    load_env_variables()
    bucket = os.environ.get("S3_RAW_BUCKET", "smart-warehouse-data-lake")
    if not bucket:
        raise ValueError(
            "[CONFIG ERROR] 'S3_RAW_BUCKET' is not defined in .env! "
            "Please configure S3_RAW_BUCKET in your .env file."
        )
    return bucket


def _get_s3_client():
    """Initializes boto3 S3 client using credentials exclusively from .env."""
    load_env_variables()
    aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    aws_region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

    if not aws_access_key or not aws_secret_key:
        raise ValueError(
            "[CONFIG ERROR] Missing AWS credentials in .env! "
            "Ensure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set in your .env file."
        )

    return boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )


def _upload_file_to_s3(s3_client, local_path: str, bucket_name: str, key_name: str):
    """Uploads a single file directly to AWS S3."""
    print(f"[AWS S3] Uploading: {local_path} -> s3://{bucket_name}/{key_name}")
    s3_client.upload_file(local_path, bucket_name, key_name)
    print(f"[AWS S3] [OK] Successfully uploaded: s3://{bucket_name}/{key_name}")


def _get_context_date(context) -> datetime:
    """Helper to get execution date safely across Airflow 2.x and 3.x."""
    return context.get("logical_date") or context.get("execution_date") or datetime.now()


# ==============================================================================
# Task 1: Generate Large Synthetic Raw Datasets with Injected Defects
# ==============================================================================
def generate_raw_data(**context):
    """Generates all 21 raw datasets locally in raw_path."""
    raw_path = _get_raw_data_path()
    dag_run = context.get("dag_run")

    profile = os.environ.get("DATA_GENERATOR_PROFILE", "full")
    seed_str = os.environ.get("DATA_GENERATOR_SEED", "42")
    seed = int(seed_str) if seed_str.isdigit() else 42
    inject_defects = os.environ.get("DATA_INJECT_DEFECTS", "true").lower() in ("true", "1", "yes")

    # Allow dynamic override via Airflow dag_run conf
    if dag_run and dag_run.conf:
        if "profile" in dag_run.conf:
            profile = dag_run.conf["profile"]
        if "seed" in dag_run.conf:
            seed = int(dag_run.conf["seed"])
        if "inject_defects" in dag_run.conf:
            inject_defects = bool(dag_run.conf["inject_defects"])

    print("================================================================================")
    print(f"[*] Task 1: Generating Raw Datasets (Profile: {profile.upper()}, Seed: {seed}, Defects: {inject_defects})")
    print(f"[-] Target Directory: {raw_path}")
    print("================================================================================")

    from data_generator.generator import run_generation
    manifest = run_generation(
        profile_name=profile,
        output_dir=raw_path,
        inject_defects=inject_defects,
        seed=seed
    )
    generated_count = len(manifest.get("generated_files", {}))
    print(f"[DATA GENERATOR] [OK] Successfully generated {generated_count} raw datasets in {raw_path}")


# ==============================================================================
# Task 2: Data Quality & Completeness Validation Checks
# ==============================================================================
def validate_raw_data_quality(**context):
    """Validates completeness, file sizes, schema columns, and defects before S3 upload."""
    raw_path = _get_raw_data_path()
    print("================================================================================")
    print(f"[*] Task 2: Executing Data Quality & Completeness Validation on {raw_path}")
    print("================================================================================")

    all_expected_files = (
        [f"{tbl}.csv" for tbl in MASTER_TABLES] +
        [f"{tbl}.csv" for tbl in TRANSACTIONAL_TABLES] +
        ["inventory_transactions.jsonl", "weather_daily.json"]
    )

    missing_files = []
    empty_files = []

    for fname in all_expected_files:
        fpath = os.path.join(raw_path, fname)
        if not os.path.exists(fpath):
            missing_files.append(fname)
        else:
            size_bytes = os.path.getsize(fpath)
            if size_bytes == 0:
                empty_files.append(fname)

    if missing_files:
        raise ValueError(f"[DQ FAIL] Missing {len(missing_files)} expected raw files: {missing_files}")
    if empty_files:
        raise ValueError(f"[DQ FAIL] Found {len(empty_files)} empty (0-byte) raw files: {empty_files}")

    print(f"[DQ PASS] Completeness Check: All {len(all_expected_files)} datasets exist with valid non-zero sizes.")

    import pandas as pd

    df_prod = pd.read_csv(os.path.join(raw_path, "products.csv"))
    print(f"[DQ CHECK] Products count: {len(df_prod):,} rows")
    has_dup_sku = df_prod["sku_code"].duplicated().any()
    print(f"  * Injected Defect Check (Duplicate SKU): {'PRESENT (Expected for Glue testing)' if has_dup_sku else 'ABSENT'}")

    df_lines = pd.read_csv(os.path.join(raw_path, "sales_order_lines.csv"))
    print(f"[DQ CHECK] Sales Order Lines count: {len(df_lines):,} rows")
    has_neg_qty = (df_lines["qty_ordered"] < 0).any()
    print(f"  * Injected Defect Check (Negative Quantity): {'PRESENT (Expected for Glue quarantine)' if has_neg_qty else 'ABSENT'}")

    df_rec = pd.read_csv(os.path.join(raw_path, "goods_receipt_lines.csv"))
    print(f"[DQ CHECK] Goods Receipts count: {len(df_rec):,} rows")
    has_bad_exp = (df_rec["expiry_date"] == "2023-01-01").any()
    print(f"  * Injected Defect Check (Expired Receipts): {'PRESENT (Expected for Glue quarantine)' if has_bad_exp else 'ABSENT'}")

    df_snap = pd.read_csv(os.path.join(raw_path, "inventory_snapshots.csv"))
    print(f"[DQ CHECK] Inventory Snapshots count: {len(df_snap):,} rows across {df_snap['product_id'].nunique()} products")

    with open(os.path.join(raw_path, "inventory_transactions.jsonl"), "r", encoding="utf-8") as f:
        first_line = json.loads(f.readline())
        print(f"[DQ CHECK] Inventory Ledger sample: TXN_ID={first_line.get('txn_id')}, REF={first_line.get('reference_id')}")

    print("================================================================================")
    print("  [SUCCESS] All Raw Data Quality Checks Passed! Ready for AWS S3 Ingestion.     ")
    print("================================================================================\n")


# ==============================================================================
# Task 3: Upload Master & Reference Tables to S3
# ==============================================================================
def ingest_master_tables(**context):
    """Uploads master & reference data with snapshot_date=YYYY-MM-DD partitioning."""
    raw_path = _get_raw_data_path()
    s3_bucket = _get_s3_bucket()
    execution_date = _get_context_date(context)
    snapshot_date_str = execution_date.strftime("%Y-%m-%d")
    s3_client = _get_s3_client()

    print(f"[*] Ingesting Master Tables to s3://{s3_bucket} (snapshot_date={snapshot_date_str})...")
    for tbl in MASTER_TABLES:
        fname = f"{tbl}.csv"
        local_file = os.path.join(raw_path, fname)
        if os.path.exists(local_file):
            s3_key = f"raw/{tbl}/snapshot_date={snapshot_date_str}/{fname}"
            _upload_file_to_s3(s3_client, local_file, s3_bucket, s3_key)
        else:
            print(f"[WARN] File not found: {local_file}")


# ==============================================================================
# Task 4: Compress and Upload Transactional Tables to S3
# ==============================================================================
def ingest_transactional_tables(**context):
    """Compresses with gzip and uploads transactional tables with year/month/day partitioning."""
    raw_path = _get_raw_data_path()
    s3_bucket = _get_s3_bucket()
    execution_date = _get_context_date(context)
    year = execution_date.strftime("%Y")
    month = execution_date.strftime("%m")
    day = execution_date.strftime("%d")
    ds_nodash = execution_date.strftime("%Y%m%d")
    s3_client = _get_s3_client()

    print(f"[*] Ingesting Transactional Tables to s3://{s3_bucket} (year={year}/month={month}/day={day})...")
    for tbl in TRANSACTIONAL_TABLES:
        fname = f"{tbl}.csv"
        local_file = os.path.join(raw_path, fname)
        if os.path.exists(local_file):
            gz_fname = f"{tbl}_{ds_nodash}.csv.gz"
            gz_local_path = f"/tmp/{gz_fname}"

            with open(local_file, "rb") as f_in, gzip.open(gz_local_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

            s3_key = f"raw/{tbl}/year={year}/month={month}/day={day}/{gz_fname}"
            _upload_file_to_s3(s3_client, gz_local_path, s3_bucket, s3_key)

            if os.path.exists(gz_local_path):
                os.remove(gz_local_path)
        else:
            print(f"[WARN] File not found: {local_file}")


# ==============================================================================
# Task 5: Upload Event Streams (JSONL) and Weather Observations (JSON)
# ==============================================================================
def ingest_event_and_weather_streams(**context):
    """Uploads JSON Lines ledger movements and daily weather observations."""
    raw_path = _get_raw_data_path()
    s3_bucket = _get_s3_bucket()
    execution_date = _get_context_date(context)
    year = execution_date.strftime("%Y")
    month = execution_date.strftime("%m")
    day = execution_date.strftime("%d")
    ds_nodash = execution_date.strftime("%Y%m%d")
    s3_client = _get_s3_client()

    txn_file = os.path.join(raw_path, "inventory_transactions.jsonl")
    if os.path.exists(txn_file):
        gz_txn_name = f"inventory_transactions_{ds_nodash}.jsonl.gz"
        gz_txn_path = f"/tmp/{gz_txn_name}"
        with open(txn_file, "rb") as f_in, gzip.open(gz_txn_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        s3_key = f"raw/inventory_transactions/year={year}/month={month}/day={day}/{gz_txn_name}"
        _upload_file_to_s3(s3_client, gz_txn_path, s3_bucket, s3_key)
        if os.path.exists(gz_txn_path):
            os.remove(gz_txn_path)
    else:
        print(f"[WARN] File not found: {txn_file}")

    weather_file = os.path.join(raw_path, "weather_daily.json")
    if os.path.exists(weather_file):
        w_fname = f"weather_daily_{ds_nodash}.json"
        s3_key = f"raw/weather_daily/year={year}/month={month}/{w_fname}"
        _upload_file_to_s3(s3_client, weather_file, s3_bucket, s3_key)
    else:
        print(f"[WARN] File not found: {weather_file}")


# ==============================================================================
# Task 6: Cleanup Local Raw Files after Successful S3 Upload
# ==============================================================================
def cleanup_local_raw_files(**context):
    """Deletes all generated raw files from local storage after successful S3 ingestion."""
    raw_path = _get_raw_data_path()
    print("================================================================================")
    print(f"[*] Task 6: Cleaning up local raw files in: {raw_path}")
    print("================================================================================")

    if not os.path.exists(raw_path):
        print(f"[CLEANUP] Path {raw_path} does not exist. Nothing to purge.")
        return

    deleted_count = 0
    for filename in os.listdir(raw_path):
        file_path = os.path.join(raw_path, filename)
        if os.path.isfile(file_path):
            try:
                os.remove(file_path)
                deleted_count += 1
                print(f"  * [DELETED] {filename}")
            except Exception as e:
                print(f"  * [ERROR] Failed to delete {filename}: {e}")

    print("================================================================================")
    print(f"  [SUCCESS] Purged {deleted_count} local files. Local storage is clean!           ")
    print("================================================================================\n")


# ==============================================================================
# Task 7: Pre-Flight Check for AWS Glue Lakehouse Jobs
# ==============================================================================
def preflight_glue_check(**context):
    """
    Validates AWS credentials and verifies that all 6 Glue jobs exist
    and are accessible before launching the DPU cluster runs.
    """
    load_env_variables()
    aws_region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    s3_bucket = _get_s3_bucket()

    print("================================================================================")
    print("  [*] Task 7: Verifying AWS Glue Lakehouse Prerequisites                        ")
    print("================================================================================")
    print(f"  * AWS Region:       {aws_region}")
    print(f"  * S3 Bucket:        s3://{s3_bucket}")
    print(f"  * AWS Credentials:  {'[CONFIGURED]' if aws_access_key and aws_secret_key else '[MISSING]'}")

    session_kwargs = {"region_name": aws_region}
    if aws_access_key and aws_secret_key:
        session_kwargs["aws_access_key_id"] = aws_access_key
        session_kwargs["aws_secret_access_key"] = aws_secret_key

    client = boto3.client("glue", **session_kwargs)

    all_jobs = GLUE_JOBS_BRONZE + GLUE_JOBS_SILVER
    print(f"\n  Checking readiness for {len(all_jobs)} AWS Glue Jobs...")

    missing_jobs = []
    for job_name in all_jobs:
        try:
            resp = client.get_job(JobName=job_name)
            job_def = resp.get("Job", {})
            glue_ver = job_def.get("GlueVersion", "N/A")
            worker_type = job_def.get("WorkerType", "Standard")
            num_workers = job_def.get("NumberOfWorkers", "N/A")
            print(f"  * [OK] {job_name:<30} (Glue {glue_ver}, {num_workers} x {worker_type})")
        except client.exceptions.EntityNotFoundException:
            missing_jobs.append(f"{job_name} (Job does not exist in AWS Glue)")
            print(f"  * [NOT FOUND] {job_name}")
        except Exception as e:
            missing_jobs.append(f"{job_name} (Error: {e})")
            print(f"  * [ERROR] {job_name}: {e}")

    if missing_jobs:
        raise ValueError(
            f"[PRE-FLIGHT ERROR] One or more AWS Glue jobs are inaccessible: {missing_jobs}"
        )

    print("================================================================================")
    print("  [PRE-FLIGHT SUCCESS] All 6 AWS Glue Jobs are verified and ready to run!        ")
    print("================================================================================\n")
    return True


# ==============================================================================
# DAG Definition & Pipeline Topology
# ==============================================================================
DEFAULT_ARGS = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="smart_warehouse_lakehouse_end_to_end",
    default_args=DEFAULT_ARGS,
    description="End-to-End: Raw Ingestion -> S3 -> Bronze (Parallel) -> Barrier -> Silver (Parallel)",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["warehouse", "lakehouse", "s3", "glue", "bronze", "silver", "iceberg", "end_to_end"],
    doc_md=__doc__,
) as dag:

    # --------------------------------------------------------------------------
    # Stage 1: Local Raw Data Generation & Validation
    # --------------------------------------------------------------------------
    t_generate = PythonOperator(
        task_id="generate_raw_data",
        python_callable=generate_raw_data,
    )

    t_validate = PythonOperator(
        task_id="validate_raw_data_quality",
        python_callable=validate_raw_data_quality,
    )

    # --------------------------------------------------------------------------
    # Stage 2: Parallel Ingestion to AWS S3
    # --------------------------------------------------------------------------
    t_master = PythonOperator(
        task_id="ingest_master_tables_to_s3",
        python_callable=ingest_master_tables,
    )

    t_transactional = PythonOperator(
        task_id="ingest_transactional_tables_to_s3",
        python_callable=ingest_transactional_tables,
    )

    t_events = PythonOperator(
        task_id="ingest_event_and_weather_streams_to_s3",
        python_callable=ingest_event_and_weather_streams,
    )

    # --------------------------------------------------------------------------
    # Stage 3: Local Storage Cleanup
    # --------------------------------------------------------------------------
    t_cleanup = PythonOperator(
        task_id="cleanup_local_raw_files",
        python_callable=cleanup_local_raw_files,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # --------------------------------------------------------------------------
    # Stage 4: Glue Lakehouse Pre-Flight Check
    # --------------------------------------------------------------------------
    t_preflight = PythonOperator(
        task_id="preflight_glue_check",
        python_callable=preflight_glue_check,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Dynamic Glue Job Script Arguments
    s3_bucket_name = os.environ.get("S3_RAW_BUCKET", "smart-warehouse-data-lake")
    glue_script_args = {
        "--RAW_S3_PATH": f"s3://{s3_bucket_name}/raw",
        "--WAREHOUSE_S3_PATH": f"s3://{s3_bucket_name}",
    }

    # --------------------------------------------------------------------------
    # Stage 5: Bronze Layer (3 AWS Glue Jobs Running in Parallel)
    # --------------------------------------------------------------------------
    t_bronze_01 = GlueJobOperator(
        task_id="glue_bronze_01_reference",
        job_name="Bronze_01_reference",
        script_args=glue_script_args,
        region_name=AWS_REGION,
        aws_conn_id=AWS_CONN_ID,
        wait_for_completion=True,
        verbose=False,
        job_poll_interval=15,
    )

    t_bronze_02 = GlueJobOperator(
        task_id="glue_bronze_02_transactions",
        job_name="Bronze_02_transactions",
        script_args=glue_script_args,
        region_name=AWS_REGION,
        aws_conn_id=AWS_CONN_ID,
        wait_for_completion=True,
        verbose=False,
        job_poll_interval=15,
    )

    t_bronze_03 = GlueJobOperator(
        task_id="glue_bronze_03_inventory_ops",
        job_name="Bronze_03_inventory_ops",
        script_args=glue_script_args,
        region_name=AWS_REGION,
        aws_conn_id=AWS_CONN_ID,
        wait_for_completion=True,
        verbose=False,
        job_poll_interval=15,
    )

    # --------------------------------------------------------------------------
    # Stage 6: Synchronization Barrier (Bronze Completed - all_success required)
    # --------------------------------------------------------------------------
    t_bronze_completed = EmptyOperator(
        task_id="bronze_layer_completed",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # --------------------------------------------------------------------------
    # Stage 7: Silver Layer (3 AWS Glue Jobs Running in Parallel)
    # --------------------------------------------------------------------------
    t_silver_01 = GlueJobOperator(
        task_id="glue_silver_01_master_commercial",
        job_name="Silver_01_master_commercial",
        script_args=glue_script_args,
        region_name=AWS_REGION,
        aws_conn_id=AWS_CONN_ID,
        wait_for_completion=True,
        verbose=False,
        job_poll_interval=15,
    )

    t_silver_02 = GlueJobOperator(
        task_id="glue_silver_02_transactions",
        job_name="Silver_02_transactions",
        script_args=glue_script_args,
        region_name=AWS_REGION,
        aws_conn_id=AWS_CONN_ID,
        wait_for_completion=True,
        verbose=False,
        job_poll_interval=15,
    )

    t_silver_03 = GlueJobOperator(
        task_id="glue_silver_03_inventory_ops",
        job_name="Silver_03_inventory_ops",
        script_args=glue_script_args,
        region_name=AWS_REGION,
        aws_conn_id=AWS_CONN_ID,
        wait_for_completion=True,
        verbose=False,
        job_poll_interval=15,
    )

    # --------------------------------------------------------------------------
    # Stage 8: Pipeline Completion Marker
    # --------------------------------------------------------------------------
    t_pipeline_completed = EmptyOperator(
        task_id="pipeline_completed",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ==========================================================================
    # Unified Pipeline Topology
    # ==========================================================================
    # 1. Generate -> Validate
    t_generate >> t_validate

    # 2. Validate -> Parallel S3 Ingestion
    t_validate >> [t_master, t_transactional, t_events]

    # 3. Parallel S3 Ingestion -> Local Cleanup -> Glue Pre-Flight
    [t_master, t_transactional, t_events] >> t_cleanup >> t_preflight

    # 4. Glue Pre-Flight -> Parallel Bronze Execution
    t_preflight >> [t_bronze_01, t_bronze_02, t_bronze_03]

    # 5. Parallel Bronze Execution -> Synchronization Barrier
    [t_bronze_01, t_bronze_02, t_bronze_03] >> t_bronze_completed

    # 6. Synchronization Barrier -> Parallel Silver Execution
    t_bronze_completed >> [t_silver_01, t_silver_02, t_silver_03]

    # 7. Parallel Silver Execution -> Pipeline Completed
    [t_silver_01, t_silver_02, t_silver_03] >> t_pipeline_completed
