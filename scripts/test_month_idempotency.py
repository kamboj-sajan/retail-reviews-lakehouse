#!/usr/bin/env python3
"""Prove on AWS that loading the same month twice changes nothing.

1. Load the month through the state machine (as the pipeline would).
2. Record the month's row counts in every table it writes (Athena), the
   total across all months, and the number of files under the month's
   folders in S3.
3. Load the same month again.
4. Record the same numbers. They must be identical; if not, exit 1.

Each step's numbers are printed, and the outcome is appended to
benchmarks/idempotency_runs.jsonl.

Usage:
    python scripts/test_month_idempotency.py --state-machine <arn> --cleaned <bucket> --curated <bucket> 2023-01
"""
import argparse
import datetime
import json
import os
import sys
import time

import boto3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from run_athena_query import fetch_rows, load_sql, run_query  # noqa: E402

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
COUNTS_SQL = os.path.join(ROOT, "sql", "checks", "month_counts.sql")
LOG = os.path.join(ROOT, "benchmarks", "idempotency_runs.jsonl")
TABLE_PREFIXES = {"cleaned": ["reviews/"],
                  "curated": ["reviews_enriched/", "category_monthly/", "category_daily/", "product_monthly/"]}


def run_month(sfn, state_machine, month, attempt):
    name = f"idempotency-{month}-{attempt}-{int(time.time())}"
    arn = sfn.start_execution(stateMachineArn=state_machine, name=name, input=json.dumps({"month": month}))["executionArn"]
    while True:
        described = sfn.describe_execution(executionArn=arn)
        if described["status"] != "RUNNING":
            seconds = (described["stopDate"] - described["startDate"]).total_seconds()
            print(f"  run {attempt}: {described['status']} in {seconds:.0f}s ({name})")
            if described["status"] != "SUCCEEDED":
                sys.exit(f"load failed; see execution {name}")
            return seconds
        time.sleep(20)


def snapshot(athena, s3, buckets, month):
    execution = run_query(athena, load_sql(COUNTS_SQL, {"month": month}), "rrl")
    if execution["Status"]["State"] != "SUCCEEDED":
        sys.exit(f"count query failed: {execution['Status'].get('StateChangeReason')}")
    rows = fetch_rows(athena, execution["QueryExecutionId"], 50)[1:]
    numbers = {measure: int(value) for measure, value in rows}
    for layer, prefixes in TABLE_PREFIXES.items():
        for prefix in prefixes:
            files = 0
            for page in s3.get_paginator("list_objects_v2").paginate(Bucket=buckets[layer], Prefix=prefix):
                files += sum(1 for o in page.get("Contents", []) if f"review_month={month}/" in o["Key"])
            numbers[f"s3 {layer}/{prefix}: files in month"] = files
    return numbers


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("month")
    parser.add_argument("--state-machine", required=True)
    parser.add_argument("--cleaned", required=True)
    parser.add_argument("--curated", required=True)
    parser.add_argument("--region", default="ap-southeast-2")
    args = parser.parse_args()

    sfn = boto3.client("stepfunctions", region_name=args.region)
    athena = boto3.client("athena", region_name=args.region)
    s3 = boto3.client("s3", region_name=args.region)
    buckets = {"cleaned": args.cleaned, "curated": args.curated}

    print(f"loading {args.month} twice")
    first_seconds = run_month(sfn, args.state_machine, args.month, 1)
    after_first = snapshot(athena, s3, buckets, args.month)
    second_seconds = run_month(sfn, args.state_machine, args.month, 2)
    after_second = snapshot(athena, s3, buckets, args.month)

    width = max(len(k) for k in after_first)
    print(f"\n{'measure':{width}}  {'after run 1':>12}  {'after run 2':>12}  same")
    for key in after_first:
        same = after_first[key] == after_second.get(key)
        print(f"{key:{width}}  {after_first[key]:>12}  {after_second.get(key, '-'):>12}  {'yes' if same else 'NO'}")
    identical = after_first == after_second
    print("\nPASS: the second load changed nothing" if identical else "\nFAIL: the second load changed the data")

    record = {"month": args.month, "tested_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
              "run_seconds": [round(first_seconds), round(second_seconds)],
              "after_first_run": after_first, "after_second_run": after_second, "identical": identical}
    os.makedirs(os.path.dirname(LOG), exist_ok=True)
    with open(LOG, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(record) + "\n")
    sys.exit(0 if identical else 1)


if __name__ == "__main__":
    main()
