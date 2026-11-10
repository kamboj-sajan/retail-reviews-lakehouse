#!/usr/bin/env python3
"""List Glue job runs with their measured DPU-seconds and cost.

Runs started by Step Functions are not recorded by run_glue_job.py, so this
reads them back from Glue itself (GetJobRuns): run time, the --month argument,
Glue's billed DPU-seconds, and cost at the price for the run's execution
class. With --by-month it adds the runs up per month, giving the cost of
loading each month.

Usage:
    python scripts/glue_run_costs.py rrl-clean-reviews rrl-build-curated --since 2026-11-10T10:50 --by-month
"""
import argparse
import collections
import datetime
import json
import os

import boto3

from run_glue_job import PRICE_PER_DPU_HOUR

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")


def runs(glue, job, since):
    for page in glue.get_paginator("get_job_runs").paginate(JobName=job):
        for run in page["JobRuns"]:
            if run["StartedOn"] >= since:
                yield run


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("jobs", nargs="+")
    parser.add_argument("--since", required=True, help="UTC, e.g. 2026-11-10T10:50")
    parser.add_argument("--by-month", action="store_true", help="add the runs up per --month argument")
    parser.add_argument("--region", default="ap-southeast-2")
    parser.add_argument("--log", help="also write the rows as JSON lines to this file")
    args = parser.parse_args()

    since = datetime.datetime.fromisoformat(args.since).replace(tzinfo=datetime.timezone.utc)
    glue = boto3.client("glue", region_name=args.region)
    rows = []
    for job in args.jobs:
        for run in runs(glue, job, since):
            klass = run.get("ExecutionClass", "STANDARD")
            dpu = run.get("DPUSeconds")
            rows.append({
                "job": job,
                "run_id": run["Id"],
                "month": run.get("Arguments", {}).get("--month", "all"),
                "state": run["JobRunState"],
                "started_utc": run["StartedOn"].astimezone(datetime.timezone.utc).isoformat(timespec="seconds"),
                "execution_seconds": run.get("ExecutionTime"),
                "dpu_seconds": dpu,
                "cost_usd": round(dpu / 3600 * PRICE_PER_DPU_HOUR[klass], 4) if dpu is not None else None,
                "execution_class": klass,
            })
    rows.sort(key=lambda r: (r["month"], r["started_utc"]))

    print(f"{'job':20} {'month':8} {'state':10} {'seconds':>8} {'DPU-s':>8} {'USD':>8}")
    for r in rows:
        print(f"{r['job']:20} {r['month']:8} {r['state']:10} {r['execution_seconds'] or 0:>8} "
              f"{r['dpu_seconds'] or 0:>8.0f} {r['cost_usd'] or 0:>8.4f}")
    total_dpu = sum(r["dpu_seconds"] or 0 for r in rows)
    total_cost = sum(r["cost_usd"] or 0 for r in rows)
    print(f"{'total':20} {'':8} {len(rows):>4} runs {'':>9} {total_dpu:>8.0f} {total_cost:>8.4f}")

    if args.by_month:
        months = collections.defaultdict(lambda: {"runs": 0, "seconds": 0, "dpu_seconds": 0.0, "cost_usd": 0.0})
        for r in rows:
            m = months[r["month"]]
            m["runs"] += 1
            m["seconds"] += r["execution_seconds"] or 0
            m["dpu_seconds"] += r["dpu_seconds"] or 0
            m["cost_usd"] += r["cost_usd"] or 0
        print(f"\n{'month':8} {'runs':>4} {'Glue seconds':>12} {'DPU-s':>8} {'USD':>8}")
        for month, m in sorted(months.items()):
            print(f"{month:8} {m['runs']:>4} {m['seconds']:>12} {m['dpu_seconds']:>8.0f} {m['cost_usd']:>8.4f}")

    if args.log:
        with open(args.log, "a", encoding="utf-8") as handle:
            for r in rows:
                handle.write(json.dumps(r) + "\n")


if __name__ == "__main__":
    main()
