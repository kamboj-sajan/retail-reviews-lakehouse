#!/usr/bin/env python3
"""Start a Glue job run, wait for it, and report what it cost.

The DPU-seconds come from Glue itself (GetJobRun). With auto scaling, Glue
bills for the workers actually in use, and DPUSeconds is that billed amount.
The cost is DPU-hours times the price for the job's execution class.

Usage:
    python scripts/run_glue_job.py rrl-clean-reviews
    python scripts/run_glue_job.py rrl-clean-reviews --arg categories=Video_Games

Each finished run is also appended as one JSON line to the file given with
--log (default: benchmarks/glue_runs.jsonl), so phase 7 can report from it.
"""
import argparse
import datetime
import json
import os
import sys
import time

import boto3

# AWS Price List API, region ap-southeast-2, Glue 6.0 and later
# (usage types APS2-ETL-DPU-Hour-Gen2 and APS2-ETL-Flex-DPU-Hour-Gen2).
PRICE_PER_DPU_HOUR = {"STANDARD": 0.308, "FLEX": 0.203}

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
DEFAULT_LOG = os.path.join(ROOT, "benchmarks", "glue_runs.jsonl")
FINISHED = {"SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR", "EXPIRED"}


def parse_arg(text):
    """'categories=Video_Games' -> ('--categories', 'Video_Games').

    Written without the leading dashes on the command line, because argparse
    would read '--categories=...' as an option of this script.
    """
    name, _, value = text.partition("=")
    if not name or name.startswith("-") or not value:
        raise argparse.ArgumentTypeError(f"expected name=value, got {text!r}")
    return f"--{name}", value


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("job_name")
    parser.add_argument("--arg", action="append", type=parse_arg, default=[],
                        help="job argument override, as name=value (repeatable)")
    parser.add_argument("--region", default="ap-southeast-2")
    parser.add_argument("--log", default=DEFAULT_LOG)
    parser.add_argument("--poll-seconds", type=int, default=30)
    parser.add_argument("--run-id", help="record an existing run instead of starting a new one")
    args = parser.parse_args()

    glue = boto3.client("glue", region_name=args.region)
    overrides = dict(args.arg)
    if args.run_id:
        run_id = args.run_id
    else:
        run_id = glue.start_job_run(JobName=args.job_name, Arguments=overrides)["JobRunId"]
        print(f"started {args.job_name} run {run_id} {overrides or ''}", flush=True)

    last_state = None
    while True:
        run = glue.get_job_run(JobName=args.job_name, RunId=run_id, PredecessorsIncluded=False)["JobRun"]
        state = run["JobRunState"]
        if state != last_state:
            stamp = datetime.datetime.now().strftime("%H:%M:%S")
            print(f"  {stamp} {state}", flush=True)
            last_state = state
        if state in FINISHED:
            break
        time.sleep(args.poll_seconds)

    # Glue fills in DPUSeconds a few seconds AFTER marking the run finished.
    for _ in range(12):
        if run.get("DPUSeconds") is not None:
            break
        time.sleep(10)
        run = glue.get_job_run(JobName=args.job_name, RunId=run_id, PredecessorsIncluded=False)["JobRun"]
    if args.run_id:
        overrides = {k: v for k, v in run.get("Arguments", {}).items() if k not in ("--JOB_ID", "--JOB_RUN_ID")}

    execution_class = run.get("ExecutionClass", "STANDARD")
    dpu_seconds = run.get("DPUSeconds")
    dpu_hours = dpu_seconds / 3600 if dpu_seconds is not None else None
    record = {
        "job": args.job_name,
        "run_id": run_id,
        "arguments": overrides,
        "state": state,
        "started_utc": run["StartedOn"].astimezone(datetime.timezone.utc).isoformat(timespec="seconds"),
        "execution_seconds": run.get("ExecutionTime"),
        "execution_class": execution_class,
        "worker_type": run.get("WorkerType"),
        "max_workers": run.get("NumberOfWorkers"),
        "dpu_seconds": dpu_seconds,
        "dpu_hours": round(dpu_hours, 4) if dpu_hours is not None else None,
        "price_per_dpu_hour": PRICE_PER_DPU_HOUR[execution_class],
        "cost_usd": round(dpu_hours * PRICE_PER_DPU_HOUR[execution_class], 4) if dpu_hours is not None else None,
        "error": run.get("ErrorMessage"),
    }
    print(json.dumps(record, indent=2))

    os.makedirs(os.path.dirname(args.log), exist_ok=True)
    with open(args.log, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(record) + "\n")

    sys.exit(0 if state == "SUCCEEDED" else 1)


if __name__ == "__main__":
    main()
