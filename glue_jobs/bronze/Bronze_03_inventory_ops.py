"""
bronze_03_inventory_ops  -  BRONZE stage (inventory ledger, snapshots, stockouts, weather)

For every table below:
  1. picks ONLY the raw S3 files that were not processed before (ledger table ops.ingest_ledger)
  2. applies the declared schema (typed) and runs Glue Data Quality (rules in lakehouse_config.py)
  3. passed rows  -> Iceberg table  bronze.<table>        (Glue Data Catalog updated automatically)
     rejected rows -> Iceberg table quarantine.rejected_rows (reason + original row as JSON)
"""

import sys
import traceback

from common_utils import init_job, run_bronze_table, write_error_report
from lakehouse_config import TABLES, BRONZE_JOBS

JOB = "bronze_03_inventory_ops"


def main():
    args, glue_context, spark, job = init_job()
    failures = []
    reports = []
    for table in BRONZE_JOBS[JOB]:
        try:
            run_bronze_table(spark, glue_context, args["RAW_S3_PATH"], table, TABLES[table])
        except Exception as exc:  # keep going with the other tables, fail at the end
            msg = f"[bronze] {table} FAILED: {exc}\n{traceback.format_exc()}"
            print(msg)
            print(msg, file=sys.stderr)  # also shows up in the Error logs
            reports.append(msg)
            failures.append(f"{table} -> {type(exc).__name__}: {str(exc)[:300]}")
    if failures:
        where = None
        try:
            where = write_error_report(JOB, "\n\n".join(reports))
        except Exception as exc:  # reporting must never hide the real error
            print(f"could not write error report: {exc}")
        raise RuntimeError(f"Bronze failed (full traceback: {where}) :: " + " | ".join(failures))
    job.commit()


if __name__ == "__main__":
    main()