#!/usr/bin/env python3
"""Run one SQL file in the project's Athena workgroup and report what it cost.

Prints the result table, then Athena's own statistics: bytes scanned, engine
time and total time. Cost is bytes scanned x $5 per TB (Sydney price, AWS
Price List API), with Athena's 10 MB minimum per query applied.

Usage:
    python scripts/run_athena_query.py sql/analysis/duplicate_keys.sql
    python scripts/run_athena_query.py sql/checks/month_counts.sql --param month=2023-01

A --param name=value replaces ${name} in the SQL file.

Each run is appended as one JSON line to benchmarks/athena_queries.jsonl.
"""
import argparse
import datetime
import json
import os
import sys
import time

import boto3

PRICE_PER_TB = 5.00             # USD, ap-southeast-2, usage type APS2-DataScannedInTB
MINIMUM_BILLED_BYTES = 10 * 1024 * 1024
ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
DEFAULT_LOG = os.path.join(ROOT, "benchmarks", "athena_queries.jsonl")


def load_sql(path, params=None):
    """Read a SQL file and fill in ${name} placeholders."""
    with open(path, encoding="utf-8") as handle:
        sql = handle.read().strip().rstrip(";")
    for name, value in (params or {}).items():
        sql = sql.replace("${" + name + "}", value)
    if "${" in sql:
        raise SystemExit(f"{path}: unfilled placeholder; pass --param name=value")
    return sql


def run_query(athena, sql, workgroup, poll_seconds=2):
    """Start a query, wait for it, return the final QueryExecution."""
    query_id = athena.start_query_execution(QueryString=sql, WorkGroup=workgroup)["QueryExecutionId"]
    while True:
        execution = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        if execution["Status"]["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return execution
        time.sleep(poll_seconds)


def fetch_rows(athena, query_id, limit):
    rows = []
    for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=query_id):
        for row in page["ResultSet"]["Rows"]:
            rows.append([c.get("VarCharValue", "") for c in row["Data"]])
            if len(rows) > limit:  # the first row is the header
                return rows
    return rows


def print_table(rows):
    widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
    for n, row in enumerate(rows):
        print("  ".join(v.rjust(w) if n and v.lstrip("-").replace(".", "").isdigit() else v.ljust(w)
                        for v, w in zip(row, widths)).rstrip())
        if n == 0:
            print("  ".join("-" * w for w in widths))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("sql_file")
    parser.add_argument("--workgroup", default="rrl")
    parser.add_argument("--region", default="ap-southeast-2")
    parser.add_argument("--max-rows", type=int, default=50)
    parser.add_argument("--label", help="name for this run in the log (default: the file name)")
    parser.add_argument("--param", action="append", default=[], help="name=value, fills ${name} in the SQL")
    parser.add_argument("--log", default=DEFAULT_LOG)
    args = parser.parse_args()

    params = dict(p.split("=", 1) for p in args.param)
    sql = load_sql(args.sql_file, params)

    athena = boto3.client("athena", region_name=args.region)
    began = datetime.datetime.now(datetime.timezone.utc)
    execution = run_query(athena, sql, args.workgroup)
    status = execution["Status"]
    stats = execution.get("Statistics", {})
    scanned = stats.get("DataScannedInBytes", 0)

    if status["State"] == "SUCCEEDED":
        print_table(fetch_rows(athena, execution["QueryExecutionId"], args.max_rows))
    else:
        print(f"{status['State']}: {status.get('StateChangeReason', '')}", file=sys.stderr)

    record = {
        "label": args.label or os.path.relpath(args.sql_file, ROOT),
        "params": params,
        "query_id": execution["QueryExecutionId"],
        "state": status["State"],
        "started_utc": began.isoformat(timespec="seconds"),
        "data_scanned_bytes": scanned,
        "engine_ms": stats.get("EngineExecutionTimeInMillis"),
        "total_ms": stats.get("TotalExecutionTimeInMillis"),
        "cost_usd": round(max(scanned, MINIMUM_BILLED_BYTES) / 1e12 * PRICE_PER_TB, 6) if scanned else 0.0,
    }
    print("\n" + json.dumps(record))
    os.makedirs(os.path.dirname(args.log), exist_ok=True)
    with open(args.log, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(record) + "\n")
    sys.exit(0 if status["State"] == "SUCCEEDED" else 1)


if __name__ == "__main__":
    main()
