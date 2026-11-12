#!/usr/bin/env python3
"""Ask the same questions of each lake layer in Athena; compare time, bytes, cost and answers.

Three questions (sql/benchmarks/), each written once per layer:

  Q1 yearly  reviews and average rating per category and year
  Q2 month   one month's reviews, rating and verified share
  Q3 text    reviews whose text mentions "battery" (needs the biggest column)

Layers: raw (the downloaded gzipped JSON), cleaned (Parquet, partitioned by
category and month), curated (pre-aggregated by month; Q1 and Q2 only).

Cheap queries run --repeats times and report the median; raw queries scan
every byte of the source (about 9.2 GB, $0.046 a time) and run once unless
--raw-repeats says otherwise. Every run is appended to
benchmarks/query_benchmarks.jsonl, and the summary is printed as a Markdown
table.

The answers are compared too: cleaned and curated must agree exactly; raw
differs by the duplicate copies and quarantined rows that cleaning removed.

Usage:
    python scripts/run_benchmarks.py                  # everything
    python scripts/run_benchmarks.py --only q2        # one question
"""
import argparse
import datetime
import json
import os
import statistics

import boto3

from run_athena_query import MINIMUM_BILLED_BYTES, PRICE_PER_TB, fetch_rows, load_sql, run_query

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
LOG = os.path.join(ROOT, "benchmarks", "query_benchmarks.jsonl")
QUESTIONS = {
    "q1": ("Q1 reviews and rating per category and year", "q1_yearly", ["raw", "cleaned", "curated"]),
    "q2": ("Q2 one month (2023-01) per category", "q2_month", ["raw", "cleaned", "curated"]),
    "q3": ("Q3 reviews mentioning \"battery\"", "q3_text", ["raw", "cleaned"]),
}


def cost(scanned):
    return round(max(scanned, MINIMUM_BILLED_BYTES) / 1e12 * PRICE_PER_TB, 6)


def run_once(athena, sql, workgroup):
    execution = run_query(athena, sql, workgroup)
    if execution["Status"]["State"] != "SUCCEEDED":
        raise SystemExit(f"query failed: {execution['Status'].get('StateChangeReason')}")
    stats = execution["Statistics"]
    rows = fetch_rows(athena, execution["QueryExecutionId"], limit=100_000)
    return {
        "query_id": execution["QueryExecutionId"],
        "data_scanned_bytes": stats["DataScannedInBytes"],
        "engine_ms": stats["EngineExecutionTimeInMillis"],
        "total_ms": stats["TotalExecutionTimeInMillis"],
        "cost_usd": cost(stats["DataScannedInBytes"]),
    }, rows


def total_reviews(rows):
    """Sum of the result's review-count column (the first one whose name starts with 'reviews')."""
    header = rows[0]
    column = next(i for i, name in enumerate(header) if name.startswith("reviews"))
    return sum(int(r[column]) for r in rows[1:])


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--only", choices=sorted(QUESTIONS), action="append")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--raw-repeats", type=int, default=1)
    parser.add_argument("--workgroup", default="rrl")
    parser.add_argument("--region", default="ap-southeast-2")
    parser.add_argument("--log", default=LOG)
    args = parser.parse_args()

    athena = boto3.client("athena", region_name=args.region)
    summary = []
    for key in args.only or sorted(QUESTIONS):
        title, stem, layers = QUESTIONS[key]
        answers = {}
        for layer in layers:
            path = os.path.join("sql", "benchmarks", f"{stem}_{layer}.sql")
            sql = load_sql(os.path.join(ROOT, path))
            runs = []
            for attempt in range(args.raw_repeats if layer == "raw" else args.repeats):
                started = datetime.datetime.now(datetime.timezone.utc)
                stats, rows = run_once(athena, sql, args.workgroup)
                record = {"question": key, "layer": layer, "sql": path, "attempt": attempt + 1,
                          "started_utc": started.isoformat(timespec="seconds"), **stats}
                with open(args.log, "a", encoding="utf-8") as handle:
                    handle.write(json.dumps(record) + "\n")
                print(f"{key} {layer:8} run {attempt + 1}: {stats['data_scanned_bytes']:>14,} bytes  "
                      f"{stats['engine_ms'] / 1000:7.1f}s  ${stats['cost_usd']:.6f}", flush=True)
                runs.append(stats)
            answers[layer] = rows
            summary.append({
                "question": title, "layer": layer, "runs": len(runs),
                "bytes": runs[0]["data_scanned_bytes"],
                "median_engine_s": round(statistics.median(r["engine_ms"] for r in runs) / 1000, 2),
                "median_total_s": round(statistics.median(r["total_ms"] for r in runs) / 1000, 2),
                "cost_usd": runs[0]["cost_usd"], "result_rows": len(rows) - 1, "reviews": total_reviews(rows),
            })
        if "curated" in answers:
            same = answers["curated"] == answers["cleaned"]
            print(f"{key}: curated answer {'identical to' if same else 'DIFFERENT FROM'} cleaned", flush=True)
            summary[-1]["same_as_cleaned"] = same

    print("\n| Question | Layer | Data scanned | Median time (s) | Cost (USD) | Reviews counted | Same answer as cleaned |")
    print("|---|---|---|---|---|---|---|")
    for s in summary:
        same = {True: "yes", False: "no"}.get(s.get("same_as_cleaned"), "")
        print(f"| {s['question']} | {s['layer']} | {s['bytes']:,} bytes | {s['median_engine_s']} | "
              f"{s['cost_usd']:.6f} | {s['reviews']:,} | {same} |")
    with open(os.path.join(ROOT, "benchmarks", "query_benchmarks_summary.json"), "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=1)


if __name__ == "__main__":
    main()
