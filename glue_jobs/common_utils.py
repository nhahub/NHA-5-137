"""
Shared helpers for the Bronze / Silver AWS Glue jobs (Iceberg lakehouse, Glue Data Catalog).

  bronze.<table>             typed rows that PASSED Glue Data Quality (+ _source_file, _ingested_at)
  quarantine.rejected_rows   every rejected row: reason + original text (row_json)
  silver.<table>             cleaned / deduplicated / flagged rows, month-partitioned for the big facts
  ops.ingest_ledger          which raw S3 files were already processed (Bronze incremental)

Bronze incremental : only raw files that are not in the ledger are read.
Silver incremental : only bronze rows with _ingested_at > max(_ingested_at) already in the silver table.
"""

import random
import sys
import time
from datetime import datetime
from functools import reduce

import boto3
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

CATALOG = "glue_catalog"
Q_TABLE = f"{CATALOG}.quarantine.rejected_rows"
LEDGER = f"{CATALOG}.ops.ingest_ledger"
DQ_COLS = ["DataQualityRulesPass", "DataQualityRulesFail", "DataQualityRulesSkip", "DataQualityEvaluationResult"]
_WAREHOUSE = {"root": None}


# ----------------------------------------------------------------------------
# Bootstrap / catalog helpers
# ----------------------------------------------------------------------------
def init_job(extra_args=None):
    """Spark + Glue context with the Iceberg catalog. Returns (args, glue_context, spark, job)."""
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_S3_PATH", "WAREHOUSE_S3_PATH"] + list(extra_args or []))
    _WAREHOUSE["root"] = args["WAREHOUSE_S3_PATH"].rstrip("/")
    conf = SparkConf()
    conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    conf.set(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    conf.set(f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    conf.set(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    conf.set(f"spark.sql.catalog.{CATALOG}.warehouse", _WAREHOUSE["root"])
    conf.set("spark.sql.session.timeZone", "UTC")
    conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    sc = SparkContext(conf=conf)
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    return args, glue_context, spark, job


def tbl(layer, name):
    return f"{CATALOG}.{layer}.{name}"


def _location(name):
    layer, table = name.split(".")[-2:]
    return f"{_WAREHOUSE['root']}/{layer}/{table}"


def table_exists(spark, name):
    """True/False for "does the table exist". Only a genuine "not found" answers False: a permission or S3 error
    must NOT look like a missing table (that made jobs try to CREATE an existing table)."""
    try:
        spark.table(name)
        return True
    except AnalysisException:
        return False
    except Exception as exc:
        msg = str(exc)
        if any(m in msg for m in ("NoSuchTable", "TABLE_OR_VIEW_NOT_FOUND", "Table or view not found",
                                  "NoSuchNamespace", "SCHEMA_NOT_FOUND")):
            return False
        raise


def table_or_empty(spark, name, ddl):
    return spark.table(name) if table_exists(spark, name) else spark.createDataFrame([], ddl)


def ensure_namespace(spark, layer):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{layer}")


def create_table(df, name, partition=None):
    w = df.writeTo(name).using("iceberg").tableProperty("format-version", "2")
    w = w.tableProperty("location", _location(name))
    if partition:
        w = w.partitionedBy(*partition)
    w.create()


def is_empty(df):
    return df.limit(1).count() == 0


def cairo_date(col):
    """Local (Cairo) calendar date of a UTC timestamp column."""
    c = F.col(col) if isinstance(col, str) else col
    return F.to_date(F.from_utc_timestamp(c, "Africa/Cairo"))


# ----------------------------------------------------------------------------
# BRONZE: raw files -> typed -> Glue Data Quality -> bronze / quarantine (Iceberg)
# ----------------------------------------------------------------------------
def list_raw_files(raw_path, table):
    p = raw_path.replace("s3://", "")
    bucket, _, key = p.partition("/")
    prefix = f"{key.strip('/')}/{table}/"
    s3 = boto3.client("s3")
    files = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            name = obj["Key"].rsplit("/", 1)[-1]
            if obj["Size"] == 0 or not name or name.startswith(("_", ".")):
                continue
            files.append(f"s3://{bucket}/{obj['Key']}")
    return files


def ledger_done(spark, table):
    if not table_exists(spark, LEDGER):
        return set()
    return {r[0] for r in spark.table(LEDGER).filter(F.col("table_name") == table).select("source_file").collect()}


def delete_by_files(spark, table, files, extra=""):
    """Idempotent re-runs: remove rows previously written for these files."""
    if not table_exists(spark, table):
        return
    for i in range(0, len(files), 200):
        in_list = ",".join("'" + f.replace("'", "''") + "'" for f in files[i:i + 200])
        spark.sql(f"DELETE FROM {table} WHERE _source_file IN ({in_list}){extra}")


_CONFLICT_MARKERS = ("Cannot commit", "base metadata location", "CommitFailedException", "AlreadyExistsException",
                     "already exists", "CommitStateUnknownException")


def is_commit_conflict(exc):
    """Iceberg/Glue optimistic-concurrency failures (PySpark cannot import the Java exception class: match the text)."""
    msg = str(exc)
    return any(m in msg for m in _CONFLICT_MARKERS)


def _append_or_create(spark, df, name, partition=None, retries=6):
    """
    Append, or create the table on first use. Tables shared by parallel jobs (ops.ingest_ledger,
    quarantine.rejected_rows) are created by whoever gets there first: the others hit a commit conflict, wait a
    random back-off, find the table and simply append.
    """
    for attempt in range(retries):
        try:
            if table_exists(spark, name):
                df.writeTo(name).append()
            else:
                create_table(df, name, partition)
            return
        except Exception as exc:
            if attempt == retries - 1 or not is_commit_conflict(exc):
                raise
            wait = min(30, 2 ** attempt) + random.random() * 2
            print(f"[iceberg] commit conflict on {name} (attempt {attempt + 1}/{retries}), retry in {wait:.1f}s: "
                  f"{str(exc)[:160]}")
            time.sleep(wait)


def ensure_shared_tables(spark):
    """Creates the two tables every Bronze job writes to, empty and with the exact schema. Run ONCE before the
    Bronze jobs (Bootstrap_00_shared_tables.py); safe to re-run."""
    for layer in ("bronze", "quarantine", "ops"):
        ensure_namespace(spark, layer)
    if not table_exists(spark, LEDGER):
        create_table(spark.createDataFrame([], "table_name string, source_file string, ingested_at timestamp, "
                                               "rows_total bigint, rows_good bigint, rows_bad bigint"), LEDGER)
    if not table_exists(spark, Q_TABLE):
        create_table(spark.createDataFrame([], "source_table string, quarantine_reason string, "
                                               "_ingested_at timestamp, _source_file string, row_json string, "
                                               "quarantined_at timestamp"), Q_TABLE)


def apply_dq(glue_context, df, ruleset, name):
    """Glue Data Quality with row-level outcomes. Returns the (persisted) outcome DataFrame."""
    from awsglue.dynamicframe import DynamicFrame
    from awsgluedq.transforms import EvaluateDataQuality

    dyf = DynamicFrame.fromDF(df, glue_context, f"dq_{name}")
    options = dict(
        frame=dyf,
        ruleset=ruleset,
        publishing_options={
            "dataQualityEvaluationContext": name,
            "enableDataQualityCloudWatchMetrics": False,
            "enableDataQualityResultsPublishing": True,
        },
        additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
    )
    # process_rows returns a collection {"ruleOutcomes", "rowLevelOutcomes"}; plain apply() returns only rule outcomes
    evaluator = EvaluateDataQuality()
    result = evaluator.process_rows(**options) if hasattr(evaluator, "process_rows") \
        else EvaluateDataQuality.apply(**options)
    return result["rowLevelOutcomes"].toDF().persist(StorageLevel.MEMORY_AND_DISK)


def write_error_report(job_name, text):
    """Saves the full error text to S3 (ops/errors/) so it can be read without digging in CloudWatch."""
    root = _WAREHOUSE["root"]
    if not root:
        return None
    bucket, _, key = root.replace("s3://", "").partition("/")
    path = f"{key.strip('/')}/ops/errors/{job_name}_{datetime.utcnow():%Y%m%d_%H%M%S}.txt".lstrip("/")
    boto3.client("s3").put_object(Bucket=bucket, Key=path, Body=text.encode("utf-8"))
    return f"s3://{bucket}/{path}"


def run_bronze_table(spark, glue_context, raw_path, name, cfg):
    from lakehouse_config import parse_ddl, build_ruleset

    for layer in ("bronze", "quarantine", "ops"):
        ensure_namespace(spark, layer)

    done = ledger_done(spark, name)
    files = [f for f in list_raw_files(raw_path, name) if f not in done]
    if not files:
        print(f"[bronze] {name}: no new files")
        return

    # ---- read raw as text, then apply the declared schema --------------------------------------
    schema = parse_ddl(cfg["schema"])
    names = [n for n, _ in schema]
    reader = spark.read.option("basePath", f"{raw_path.rstrip('/')}/{name}/")
    if cfg["fmt"] == "csv":
        raw = reader.option("header", True).csv(files)
    else:
        str_schema = StructType([StructField(n, StringType()) for n in names])
        raw = reader.schema(str_schema).option("multiLine", cfg.get("multiline", False)).json(files)
    raw = ensure_cols(raw, {n: "string" for n in names})
    if cfg.get("snap"):
        raw = ensure_cols(raw, {"snapshot_date": "string"})

    src_file = F.regexp_replace(
        F.input_file_name(), "^s3[an]?://", "s3://")
    cols = [F.col(n).cast(t).alias(n) for n, t in schema]
    if cfg.get("snap"):
        cols.append(F.col("snapshot_date").cast("date").alias("source_snapshot_date"))
    typed = raw.select(
        *cols,
        F.to_json(F.struct(*[F.col(n) for n in names])).alias("_raw_row"),
        src_file.alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )
    for cname, expr in cfg.get("checks", {}).items():
        typed = typed.withColumn(cname, F.coalesce(F.expr(expr), F.lit(False)).cast("int"))

    now = datetime.utcnow()
    files_df = spark.createDataFrame([(f,) for f in files], "source_file string")

    if is_empty(typed):
        ledger = (files_df.withColumn("table_name", F.lit(name)).withColumn("ingested_at", F.lit(now))
                  .withColumn("rows_total", F.lit(0).cast("long")).withColumn("rows_good", F.lit(0).cast("long"))
                  .withColumn("rows_bad", F.lit(0).cast("long")))
        _append_or_create(spark, ledger.select("table_name", "source_file", "ingested_at", "rows_total",
                                               "rows_good", "rows_bad"), LEDGER)
        print(f"[bronze] {name}: files without rows")
        return

    # ---- Glue Data Quality ---------------------------------------------------------------------
    print(f"[bronze] {name}: step 1/3 schema applied, running Glue Data Quality ...")
    rows = apply_dq(glue_context, typed, build_ruleset(cfg), f"bronze_{name}")
    print(f"[bronze] {name}: step 2/3 data quality evaluated, rows={rows.count()}")
    ok = F.col("DataQualityEvaluationResult") == "Passed"
    chk_cols = [c for c in rows.columns if c.startswith("chk_")]

    good = rows.filter(ok).drop(*DQ_COLS, "_raw_row", *chk_cols)
    bad = (rows.filter(~ok)
           .withColumn("_dq_reason", F.concat(F.lit("DQ_FAILED: "), F.concat_ws("; ", F.col("DataQualityRulesFail"))))
           .drop(*DQ_COLS))
    envelope = _envelope(bad, F.col("_dq_reason"), f"bronze.{name}")
    stats = rows.groupBy("_source_file").agg(
        F.count(F.lit(1)).alias("rows_total"), F.sum(F.when(ok, 1).otherwise(0)).alias("rows_good"))
    ledger = (files_df.join(stats, files_df.source_file == stats._source_file, "left")
              .select(F.lit(name).alias("table_name"), "source_file", F.lit(now).alias("ingested_at"),
                      F.coalesce("rows_total", F.lit(0)).cast("long").alias("rows_total"),
                      F.coalesce("rows_good", F.lit(0)).cast("long").alias("rows_good"))
              .withColumn("rows_bad", F.col("rows_total") - F.col("rows_good")))

    # ---- writes (delete-then-insert per file = safe to re-run), ledger last ---------------------
    print(f"[bronze] {name}: step 3/3 writing bronze / quarantine / ledger ...")
    target = tbl("bronze", name)
    delete_by_files(spark, target, files)
    delete_by_files(spark, Q_TABLE, files, f" AND source_table = 'bronze.{name}'")
    _append_or_create(spark, good, target, [F.days("_ingested_at")] if cfg.get("part") else None)
    _append_or_create(spark, envelope, Q_TABLE)
    _append_or_create(spark, ledger, LEDGER)

    s = ledger.agg(F.sum("rows_total"), F.sum("rows_good"), F.sum("rows_bad")).first()
    print(f"[bronze] {name}: {len(files)} file(s), rows={s[0]}, good={s[1]}, quarantined={s[2]}")
    rows.unpersist()


# ----------------------------------------------------------------------------
# SILVER: incremental reads, helpers, writes
# ----------------------------------------------------------------------------
def max_ingested(spark, name):
    """max(_ingested_at) as a string (session time zone), or None."""
    if not table_exists(spark, name):
        return None
    return spark.table(name).agg(F.max("_ingested_at").cast("string")).first()[0]


def read_new(spark, bronze_table, silver_name):
    """Bronze rows newer than what the silver table already holds -> (df, watermark)."""
    wm = max_ingested(spark, silver_name)
    df = spark.table(tbl("bronze", bronze_table))
    if wm is not None:
        df = df.filter(F.expr(f"_ingested_at > TIMESTAMP '{wm}'"))
    return df, wm


def has_new_data(spark, bronze_table, silver_name):
    b = max_ingested(spark, tbl("bronze", bronze_table))
    if b is None:
        return False
    s = max_ingested(spark, tbl("silver", silver_name))
    return s is None or b > s


def dedup_latest(df, keys, order_col="updated_ts"):
    """Newest row per key (ties: newest _ingested_at)."""
    order = F.col(order_col)
    if dict(df.dtypes).get(order_col) == "string":
        order = F.to_timestamp(order)
    orders = [order.desc()]
    if "_ingested_at" in df.columns and order_col != "_ingested_at":
        orders.append(F.col("_ingested_at").desc())
    w = Window.partitionBy(*keys).orderBy(*orders)
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def ensure_cols(df, spec):
    for name, dtype in spec.items():
        if name not in df.columns:
            df = df.withColumn(name, F.lit(None).cast(dtype))
    return df


def clean_str(name):
    """trim + blank -> NULL"""
    return F.when(F.trim(F.col(name)) == "", F.lit(None).cast("string")).otherwise(F.trim(F.col(name)))


def add_overlap_group(df, part_cols, start_col, end_col, far):
    """_grp: intervals that overlap (per partition) share an id. NULL end = open-ended."""
    d = df.withColumn("_end_f", F.coalesce(F.col(end_col), far))
    base = Window.partitionBy(*part_cols).orderBy(F.col(start_col), F.col("_end_f"))
    d = d.withColumn("_prev_max_end", F.max("_end_f").over(base.rowsBetween(Window.unboundedPreceding, -1)))
    d = d.withColumn("_new", F.when(F.col("_prev_max_end").isNull() | (F.col(start_col) > F.col("_prev_max_end")),
                                    1).otherwise(0))
    d = d.withColumn("_grp", F.sum("_new").over(base.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
    return d.drop("_prev_max_end", "_new")


def validate_ean13_udf():
    def is_valid_ean13(code):
        if not code or len(code) != 13 or not code.isdigit():
            return False
        digits = [int(d) for d in code]
        total = sum(digits[0:12:2]) + sum(digits[1:12:2]) * 3
        return (10 - (total % 10)) % 10 == digits[-1]

    return F.udf(is_valid_ean13, BooleanType())


# ---- quarantine (used by Bronze DQ and by Silver cross-table rules) --------------------------
def _envelope(bad_df, reason, source_table):
    cols = [c for c in bad_df.columns if c not in ("_dq_reason", "_raw_row")]
    reason_col = F.lit(reason) if isinstance(reason, str) else reason
    row_json = (F.col("_raw_row") if "_raw_row" in bad_df.columns
                else F.to_json(F.struct(*[F.col(c) for c in cols])))
    src_file = F.col("_source_file") if "_source_file" in bad_df.columns else F.lit(None).cast("string")
    ingested = F.col("_ingested_at") if "_ingested_at" in bad_df.columns else F.lit(None).cast("timestamp")
    return bad_df.select(
        F.lit(source_table).alias("source_table"),
        reason_col.alias("quarantine_reason"),
        ingested.alias("_ingested_at"),
        src_file.alias("_source_file"),
        row_json.alias("row_json"),
        F.current_timestamp().alias("quarantined_at"),
    )


class QBin:
    """Silver cross-table rejections (e.g. return for an order that does not exist)."""

    def __init__(self):
        self.frames = {}

    def split(self, df, condition, reason, source_table):
        cond = F.coalesce(condition, F.lit(False))
        self.frames.setdefault(source_table, []).append(_envelope(df.filter(cond), reason, source_table))
        return df.filter(~cond)

    def flush(self, spark, source_table, wm=None):
        frames = self.frames.pop(source_table, [])
        if not frames:
            return
        ensure_namespace(spark, "quarantine")
        env = reduce(DataFrame.unionByName, frames)
        if table_exists(spark, Q_TABLE):
            cond = f"source_table = '{source_table}'"
            if wm:
                cond += f" AND _ingested_at > TIMESTAMP '{wm}'"
            spark.sql(f"DELETE FROM {Q_TABLE} WHERE {cond}")
        _append_or_create(spark, env, Q_TABLE)


# ---- Iceberg writes ---------------------------------------------------------------------------
def upsert_iceberg(spark, df, target, keys, order_col=None, update=True, partition=None):
    if not table_exists(spark, target):
        create_table(df, target, partition)
        return
    df.createOrReplaceTempView("_src")
    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    matched = ""
    if update:
        guard = f" AND s.`{order_col}` >= t.`{order_col}`" if order_col else ""
        matched = f"WHEN MATCHED{guard} THEN UPDATE SET * "
    spark.sql(f"MERGE INTO {target} t USING _src s ON {on} {matched}WHEN NOT MATCHED THEN INSERT *")


def overwrite_keys_source(existing, df, row_keys, key_col):
    """
    Source for the atomic 'replace every row of these keys' MERGE:
      * new rows                                   -> _del = False  (insert / update)
      * existing rows of the same key_col values that are NOT in df (e.g. an episode id that was merged away)
                                                   -> _del = True   (delete)
    """
    cols = df.columns
    stale = (existing.join(df.select(key_col).distinct(), key_col, "left_semi")
             .join(df.select(*row_keys), row_keys, "left_anti")
             .select(*cols).withColumn("_del", F.lit(True)))
    return df.withColumn("_del", F.lit(False)).unionByName(stale)


def overwrite_keys_iceberg(spark, df, target, row_keys, key_col):
    """
    Replace ALL rows of every key_col value present in df, in ONE atomic MERGE commit.
    (df.writeTo(t).overwrite(<filter on a non-partition column>) can only delete whole files, so it fails with
    'Cannot delete file where some, but not all, rows match filter' as soon as a file holds other keys.)
    row_keys = columns that identify one row inside a key (e.g. ["stockout_id"]).
    """
    if not row_keys:
        raise ValueError("overwrite_keys needs row_keys (the `keys` argument of publish_silver)")
    cols = df.columns
    overwrite_keys_source(spark.table(target), df, row_keys, key_col).createOrReplaceTempView("_src_keys")
    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in row_keys)
    sets = ", ".join(f"t.`{c}` = s.`{c}`" for c in cols)
    ins_cols = ", ".join(f"`{c}`" for c in cols)
    ins_vals = ", ".join(f"s.`{c}`" for c in cols)
    spark.sql(f"MERGE INTO {target} t USING _src_keys s ON {on} "
              f"WHEN MATCHED AND s._del THEN DELETE "
              f"WHEN MATCHED THEN UPDATE SET {sets} "
              f"WHEN NOT MATCHED AND NOT s._del THEN INSERT ({ins_cols}) VALUES ({ins_vals})")


def publish_silver(spark, df, table, keys, mode, q, wm=None, order_col=None, key_col=None, partition=None):
    """
    mode  "upsert"          MERGE on keys (update only if order_col is newer)
          "insert_only"     MERGE on keys, existing rows untouched
          "replace"         table fully replaced (small dimensions recomputed from Bronze)
          "overwrite_keys"  rows whose key_col appears in df are replaced atomically (one MERGE; `keys` = row id columns)
    """
    ensure_namespace(spark, "silver")
    target = tbl("silver", table)
    if mode != "replace" and is_empty(df):
        q.flush(spark, f"silver.{table}", wm)
        return
    q.flush(spark, f"silver.{table}", wm)

    if mode == "replace":
        w = df.writeTo(target).using("iceberg").tableProperty("format-version", "2")
        w = w.tableProperty("location", _location(target))
        w.createOrReplace()
    elif mode == "upsert":
        upsert_iceberg(spark, df, target, keys, order_col, True, partition)
    elif mode == "insert_only":
        upsert_iceberg(spark, df, target, keys, None, False, partition)
    elif mode == "overwrite_keys":
        if table_exists(spark, target):
            overwrite_keys_iceberg(spark, df, target, keys, key_col)
        else:
            create_table(df, target, partition)
    else:
        raise ValueError(mode)
    print(f"[silver] {table}: {mode} done")
