#!/usr/bin/env python3
"""Prove on AWS that the quality gate quarantines bad rows and stops bad loads.

Adds a file of deliberately bad review lines to one month's landing folder,
next to that month's real files, and loads the month through the real state
machine, three times:

1. a few bad rows: they must be quarantined, and the load must SUCCEED;
2. many bad rows (over the 1% quarantine-rate limit): the CheckQuality step
   must FAIL, so the execution fails and the alert email is sent;
3. the bad file removed: the load must SUCCEED again with nothing quarantined.

The bad rows are made up and clearly marked (user_id "INJECTED-...", file
injected-bad-rows.txt.gz); step 3 removes them. Results are printed and
appended to benchmarks/fault_injection_runs.jsonl.

Usage:
    python scripts/fault_injection_test.py --raw <bucket> --artifacts <bucket> --state-machine <arn> --month 2023-09
"""
import argparse
import datetime
import gzip
import io
import json
import os
import sys
import time

import boto3

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
LOG = os.path.join(ROOT, "benchmarks", "fault_injection_runs.jsonl")
INJECTED_KEY = "landing/reviews/category=Video_Games/review_month={month}/injected-bad-rows.txt.gz"


def month_start_ms(month):
    return int(datetime.datetime.strptime(month, "%Y-%m").replace(tzinfo=datetime.timezone.utc).timestamp() * 1000)


def bad_lines(month, count):
    """count bad review lines, cycling through six kinds of problem."""
    start = month_start_ms(month)
    good = {"asin": "B000INJECT", "parent_asin": "B000INJECT", "rating": 5.0, "title": "injected", "text": "injected",
            "timestamp": start + 3_600_000, "helpful_vote": 0, "verified_purchase": False, "images": []}
    kinds = [
        lambda i: '{"user_id": "INJECTED-%d", "rating": ' % i,                                      # unparseable_json
        lambda i: json.dumps(dict(good, user_id=None)),                                             # missing_key
        lambda i: json.dumps(dict(good, user_id=f"INJECTED-{i}", rating=0.0)),                      # rating_out_of_range
        lambda i: json.dumps(dict(good, user_id=f"INJECTED-{i}", rating=4.5)),                      # rating_not_whole_number
        lambda i: json.dumps(dict(good, user_id=f"INJECTED-{i}", timestamp=start - 86_400_000)),    # outside_month
        lambda i: json.dumps(dict(good, user_id=f"INJECTED-{i}", timestamp=-1)),                    # timestamp_out_of_range
    ]
    return [kinds[i % len(kinds)](i) for i in range(count)]


def put_bad_rows(s3, bucket, month, count):
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as handle:
        handle.write(("\n".join(bad_lines(month, count)) + "\n").encode())
    s3.put_object(Bucket=bucket, Key=INJECTED_KEY.format(month=month), Body=buffer.getvalue())


def load(sfn, state_machine, month, label):
    name = f"fault-{label}-{month}-{int(time.time())}"
    arn = sfn.start_execution(stateMachineArn=state_machine, name=name, input=json.dumps({"month": month}))["executionArn"]
    while True:
        described = sfn.describe_execution(executionArn=arn)
        if described["status"] != "RUNNING":
            break
        time.sleep(20)
    failed_state = None
    if described["status"] != "SUCCEEDED":
        for event in sfn.get_execution_history(executionArn=arn, reverseOrder=True, maxResults=50)["events"]:
            if event["type"] == "TaskStateEntered" and event["stateEnteredEventDetails"]["name"] != "NotifyFailure":
                failed_state = event["stateEnteredEventDetails"]["name"]
                break
    return described["status"], failed_state, name


def latest_record(s3, bucket, job, month, after):
    """The newest metrics record a job wrote for month, started after `after`."""
    best = None
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"metrics/job_name={job}/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json") and obj["LastModified"] >= after:
                record = json.loads(s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read())
                if record.get("month") == month and (best is None or record["started_utc"] > best["started_utc"]):
                    best = record
    return best or {}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--raw", required=True)
    parser.add_argument("--artifacts", required=True)
    parser.add_argument("--state-machine", required=True)
    parser.add_argument("--month", default="2023-09")
    parser.add_argument("--few", type=int, default=20)
    parser.add_argument("--many", type=int, default=200)
    parser.add_argument("--region", default="ap-southeast-2")
    args = parser.parse_args()

    s3 = boto3.client("s3", region_name=args.region)
    sfn = boto3.client("stepfunctions", region_name=args.region)
    steps = [("few", args.few, "SUCCEEDED"), ("many", args.many, "FAILED"), ("removed", 0, "SUCCEEDED")]
    results, ok = [], True

    for label, count, expected in steps:
        if count:
            put_bad_rows(s3, args.raw, args.month, count)
        else:
            s3.delete_object(Bucket=args.raw, Key=INJECTED_KEY.format(month=args.month))
        began = datetime.datetime.now(datetime.timezone.utc)
        print(f"step {label}: {count} bad rows injected, loading {args.month} ...", flush=True)
        status, failed_state, name = load(sfn, args.state_machine, args.month, label)
        clean = latest_record(s3, args.artifacts, "clean_reviews", args.month, began)
        quality = latest_record(s3, args.artifacts, "check_quality", args.month, began)
        failed_checks = [c["name"] for c in json.loads(quality.get("checks_json", "[]")) if not c["passed"]]
        row = {
            "step": label, "injected": count, "expected": expected, "status": status, "failed_state": failed_state,
            "landing_lines": quality.get("landing_lines"), "cleaned_rows": quality.get("cleaned_rows"),
            "quarantined_rows": clean.get("quarantined_rows"),
            "by_reason": {k.replace("quarantined_", ""): v for k, v in clean.items()
                          if k.startswith("quarantined_") and k != "quarantined_rows" and v},
            "checks_failed": failed_checks, "execution": name,
        }
        ok &= status == expected and (count == 0 or clean.get("quarantined_rows") == count)
        results.append(row)
        print(json.dumps(row, indent=2), flush=True)

    print("\nPASS: bad rows were quarantined, and too many stopped the load" if ok else "\nFAIL: see the steps above")
    with open(LOG, "a", encoding="utf-8") as handle:
        handle.write(json.dumps({"month": args.month, "tested_utc": datetime.datetime.now(datetime.timezone.utc)
                                 .isoformat(timespec="seconds"), "passed": ok, "steps": results}) + "\n")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
