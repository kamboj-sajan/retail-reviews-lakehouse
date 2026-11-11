#!/usr/bin/env python3
"""Run one SQL file in the Redshift warehouse and report how long it took.

Runs as the admin user (through the Data API, which reads the password from
Secrets Manager itself). Prints the result table and Redshift's own duration
for the statement. There is no per-query price as with Athena: Redshift
Serverless bills RPU-seconds while queries run, which
sql/warehouse/checks/usage.sql reads back.

Usage:
    python scripts/run_redshift_query.py sql/warehouse/checks/table_counts.sql \\
        --workgroup rrl-warehouse --database reviews --secret-arn <arn>

A --param name=value replaces ${name} in the SQL file. Each run is appended
as one JSON line to benchmarks/redshift_queries.jsonl.
"""
import argparse
import datetime
import json
import os
import sys

import boto3

from run_athena_query import load_sql, print_table

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, os.path.join(ROOT, "src"))
DEFAULT_LOG = os.path.join(ROOT, "benchmarks", "redshift_queries.jsonl")

from lakehouse.warehouse import RedshiftData, StatementFailed  # noqa: E402


def as_table(rows, limit):
    if not rows:
        return [["(no rows)"]]
    header = list(rows[0])
    return [header] + [["" if r[c] is None else str(r[c]) for c in header] for r in rows[:limit]]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("sql_file")
    parser.add_argument("--workgroup", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--secret-arn", required=True)
    parser.add_argument("--region", default="ap-southeast-2")
    parser.add_argument("--max-rows", type=int, default=50)
    parser.add_argument("--label", help="name for this run in the log (default: the file name)")
    parser.add_argument("--param", action="append", default=[], help="name=value, fills ${name} in the SQL")
    parser.add_argument("--log", default=DEFAULT_LOG)
    args = parser.parse_args()

    params = dict(p.split("=", 1) for p in args.param)
    sql = load_sql(args.sql_file, params)
    client = boto3.client("redshift-data", region_name=args.region)
    redshift = RedshiftData(client, args.workgroup, args.database, secret_arn=args.secret_arn)

    began = datetime.datetime.now(datetime.timezone.utc)
    state, error, rows = "FINISHED", None, []
    try:
        rows = redshift.query(sql)
    except StatementFailed as failure:
        state, error = "FAILED", str(failure)
    if rows or state == "FINISHED":
        print_table(as_table(rows, args.max_rows))
    if error:
        print(error, file=sys.stderr)

    statement = redshift.last_statement or {}
    record = {
        "label": args.label or os.path.relpath(args.sql_file, ROOT),
        "params": params,
        "statement_id": statement.get("Id"),
        "state": state,
        "started_utc": began.isoformat(timespec="seconds"),
        "duration_ms": round(statement.get("Duration", 0) / 1e6) if statement.get("Duration", -1) >= 0 else None,
        "result_rows": statement.get("ResultRows"),
    }
    print("\n" + json.dumps(record))
    os.makedirs(os.path.dirname(args.log), exist_ok=True)
    with open(args.log, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(record) + "\n")
    sys.exit(0 if state == "FINISHED" else 1)


if __name__ == "__main__":
    main()
