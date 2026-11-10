#!/usr/bin/env python3
"""Deliver one month of reviews, as if it had just arrived from the source.

The dataset is static, so arrival is simulated:
1. copy raw/feed/reviews/category=*/review_month=<MONTH>/* (written once by
   the split_raw_by_month Glue job) to raw/landing/reviews/category=*/review_month=<MONTH>/;
2. then write the marker raw/landing/_arrived/<MONTH>.

The marker's S3 event reaches EventBridge, which starts the monthly-load
state machine (infra/terraform/orchestration.tf). The marker is written last,
so the load never starts on a half-copied month. Copies are server-side
(S3 CopyObject): no data passes through this machine.

Delivering the same month again replaces its landing files exactly, so a
re-delivery is a clean re-run.

Usage:
    python scripts/replay_month.py --bucket <raw bucket> 2023-01
    python scripts/replay_month.py --bucket <raw bucket> --state-machine <arn> --wait 2022-10 2022-11 2022-12
"""
import argparse
import datetime
import json
import sys
import time

import boto3

FEED = "feed/reviews/"
LANDING = "landing/reviews/"
ARRIVED = "landing/_arrived/"


def month_keys(s3, bucket, prefix, month):
    """Every object key under prefix that belongs to review_month=<month>."""
    marker = f"/review_month={month}/"
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", []) if marker in o["Key"]]
    return keys


def deliver(s3, bucket, month):
    sources = month_keys(s3, bucket, FEED, month)
    if not sources:
        sys.exit(f"nothing in s3://{bucket}/{FEED} for {month}; has split_raw_by_month run?")
    targets = {LANDING + key[len(FEED):]: key for key in sources}

    # Remove landing files that the feed no longer has, so the landing folder
    # matches the feed exactly after a re-delivery.
    for stale in set(month_keys(s3, bucket, LANDING, month)) - set(targets):
        s3.delete_object(Bucket=bucket, Key=stale)
    for target, source in sorted(targets.items()):
        s3.copy({"Bucket": bucket, "Key": source}, bucket, target)
        print(f"  copied {target}")

    marker = f"{ARRIVED}{month}"
    body = {"month": month, "files": sorted(targets),
            "delivered_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")}
    s3.put_object(Bucket=bucket, Key=marker, Body=json.dumps(body, indent=2).encode(), ContentType="application/json")
    print(f"  marker s3://{bucket}/{marker}")
    return marker


def wait_for_execution(sfn, state_machine, marker, since, timeout_seconds=7200):
    """Find the execution EventBridge started for this marker and wait for it."""
    deadline = time.monotonic() + timeout_seconds
    execution_arn = None
    while execution_arn is None:
        if time.monotonic() > deadline:
            sys.exit("no execution started for the marker; check the EventBridge rule")
        for execution in sfn.list_executions(stateMachineArn=state_machine, maxResults=20)["executions"]:
            if execution["startDate"] >= since:
                described = sfn.describe_execution(executionArn=execution["executionArn"])
                if json.loads(described.get("input", "{}")).get("key") == marker:
                    execution_arn = execution["executionArn"]
                    break
        else:
            time.sleep(5)
    while True:
        described = sfn.describe_execution(executionArn=execution_arn)
        if described["status"] != "RUNNING":
            seconds = (described["stopDate"] - described["startDate"]).total_seconds()
            return described["status"], seconds, execution_arn.rsplit(":", 1)[1]
        if time.monotonic() > deadline:
            sys.exit("timed out waiting for the execution")
        time.sleep(20)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("months", nargs="+", help="one or more YYYY-MM")
    parser.add_argument("--bucket", required=True, help="raw bucket (terraform output raw_bucket)")
    parser.add_argument("--state-machine", help="state machine ARN; needed with --wait")
    parser.add_argument("--wait", action="store_true", help="wait for the loads these deliveries trigger")
    parser.add_argument("--region", default="ap-southeast-2")
    args = parser.parse_args()

    s3 = boto3.client("s3", region_name=args.region)
    since = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=5)
    markers = {}
    for month in args.months:
        print(f"delivering {month}")
        markers[month] = deliver(s3, args.bucket, month)
    if not args.wait:
        return
    if not args.state_machine:
        sys.exit("--wait needs --state-machine")

    # The loads run in parallel (Glue allows a few runs at a time; the rest
    # wait in the state machine's retry loop). Waiting on them one by one is
    # fine: the total wait is the time until the last one finishes.
    sfn = boto3.client("stepfunctions", region_name=args.region)
    results = {month: wait_for_execution(sfn, args.state_machine, marker, since) for month, marker in markers.items()}
    print(f"\n{'month':8} {'status':10} {'seconds':>8}  execution")
    for month, (status, seconds, name) in results.items():
        print(f"{month:8} {status:10} {seconds:>8.0f}  {name}")
    sys.exit(0 if all(status == "SUCCEEDED" for status, _, _ in results.values()) else 1)


if __name__ == "__main__":
    main()
