#!/usr/bin/env python3
"""Build the dashboard: query the curated tables in Athena, write one HTML page.

The page answers the three questions the curated tables were built for
(docs/data-lake-layout.md, "Aggregates"):

1. How do ratings trend, per category?           category_monthly
2. What happens to review volume around sales?   category_daily
3. Which products' ratings are falling?          product_monthly

Each query reads only aggregate tables: the build scans about 223 MB, almost
all of it product_monthly (cost about $0.001, recorded in
benchmarks/athena_queries.jsonl).
The page is self-contained: data, styles and chart code in one file, no
server and no external requests, so it opens from disk.

Output goes to data/dashboard/ (git-ignored, like all data in this repo):
    data.json     every query result, as fetched
    index.html    the dashboard

Usage:
    python scripts/build_dashboard.py                 # query Athena, then render
    python scripts/build_dashboard.py --from-json     # re-render from data.json, no queries
"""
import argparse
import datetime
import json
import os
import statistics
import sys

import boto3

from run_athena_query import DEFAULT_LOG, MINIMUM_BILLED_BYTES, PRICE_PER_TB, fetch_rows, load_sql, run_query

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
TEMPLATE = os.path.join(ROOT, "dashboard", "template.html")
OUT_DIR = os.path.join(ROOT, "data", "dashboard")

# Daily panels cover the last full years before the dataset ends (2023-09).
DAILY_FROM, DAILY_TO = "2019-01", "2023-09"

# "Falling ratings": the 6 months before review volume starts tapering off as
# the dataset's collection ends (2023-04 onwards has far fewer reviews),
# against the 12 months before that.
WINDOWS = {"before_from": "2021-10", "before_to": "2022-09", "recent_from": "2022-10", "recent_to": "2023-03"}
MIN_REVIEWS = 50

# Sale periods, first and last day (US dates, as Amazon announced them).
# Prime Day 2023 (11-12 July) is left out: the 28 days after it run into
# August and September 2023, when the dataset is already tapering off, so
# "after" would be measuring the end of collection, not the sale.
SALES = [
    ("Prime Day 2019", "2019-07-15", "2019-07-16"),
    ("Black Friday 2019", "2019-11-29", "2019-12-02"),
    ("Prime Day 2020", "2020-10-13", "2020-10-14"),
    ("Black Friday 2020", "2020-11-27", "2020-11-30"),
    ("Prime Day 2021", "2021-06-21", "2021-06-22"),
    ("Black Friday 2021", "2021-11-26", "2021-11-29"),
    ("Prime Day 2022", "2022-07-12", "2022-07-13"),
    ("Black Friday 2022", "2022-11-25", "2022-11-28"),
]
COMPARE_DAYS = 28  # days before the sale vs days after it

QUERIES = [
    ("category_monthly", "sql/dashboard/category_monthly.sql", {}),
    ("category_daily", "sql/dashboard/category_daily.sql", {"from": DAILY_FROM, "to": DAILY_TO}),
    ("falling_products", "sql/dashboard/falling_products.sql", dict(WINDOWS, min_reviews=str(MIN_REVIEWS))),
]


def typed(value):
    """Athena returns text; turn numbers back into numbers."""
    if value == "":
        return None
    for kind in (int, float):
        try:
            return kind(value)
        except ValueError:
            pass
    return value


def as_dicts(rows):
    header, *body = rows
    return [{h: typed(v) for h, v in zip(header, row)} for row in body]


def query_all(region, workgroup, log_path):
    athena = boto3.client("athena", region_name=region)
    results, runs = {}, []
    for name, path, params in QUERIES:
        execution = run_query(athena, load_sql(os.path.join(ROOT, path), params), workgroup)
        if execution["Status"]["State"] != "SUCCEEDED":
            sys.exit(f"{path}: {execution['Status']['State']} {execution['Status'].get('StateChangeReason', '')}")
        results[name] = as_dicts(fetch_rows(athena, execution["QueryExecutionId"], limit=1_000_000))
        scanned = execution["Statistics"]["DataScannedInBytes"]
        runs.append({
            "label": f"dashboard: {path}", "params": params, "query_id": execution["QueryExecutionId"],
            "state": "SUCCEEDED", "started_utc": execution["Status"]["SubmissionDateTime"].isoformat(timespec="seconds"),
            "data_scanned_bytes": scanned, "engine_ms": execution["Statistics"].get("EngineExecutionTimeInMillis"),
            "total_ms": execution["Statistics"].get("TotalExecutionTimeInMillis"),
            "cost_usd": round(max(scanned, MINIMUM_BILLED_BYTES) / 1e12 * PRICE_PER_TB, 6),
        })
    with open(log_path, "a", encoding="utf-8") as handle:
        for run in runs:
            handle.write(json.dumps(run) + "\n")
    return results, runs


# --- derived numbers ---------------------------------------------------------------


def day(text):
    return datetime.date.fromisoformat(text)


def sale_effects(daily, sales=SALES, days=COMPARE_DAYS):
    """Average reviews per day in the `days` before each sale and the `days` after it.

    Before: the days ending the day before the sale starts. After: the days
    starting the day after it ends. The sale days themselves are in neither.
    Returns one row per sale and category, with the ratio after / before.
    """
    counts = {}
    for row in daily:
        counts.setdefault(row["category"], {})[day(row["review_date"])] = row["review_count"]
    out = []
    for name, first, last in sales:
        first, last = day(first), day(last)
        for category in sorted(counts):
            by_day = counts[category]
            before = [by_day.get(first - datetime.timedelta(days=n), 0) for n in range(1, days + 1)]
            after = [by_day.get(last + datetime.timedelta(days=n), 0) for n in range(1, days + 1)]
            before_avg, after_avg = statistics.fmean(before), statistics.fmean(after)
            out.append({"sale": name, "first_day": first.isoformat(), "last_day": last.isoformat(),
                        "category": category, "before_per_day": round(before_avg, 1),
                        "after_per_day": round(after_avg, 1),
                        "ratio": round(after_avg / before_avg, 3) if before_avg else None})
    return out


def headline(monthly):
    """Totals across every month and category."""
    reviews = sum(r["review_count"] for r in monthly)
    rating_sum = sum(r["rating_sum"] for r in monthly)
    months = sorted({r["review_month"] for r in monthly})
    per_category = {}
    for r in monthly:
        c = per_category.setdefault(r["category"], {"reviews": 0, "rating_sum": 0, "verified": 0})
        c["reviews"] += r["review_count"]
        c["rating_sum"] += r["rating_sum"]
        c["verified"] += r["verified_count"]
    return {
        "reviews": reviews,
        "avg_rating": round(rating_sum / reviews, 3),
        "first_month": months[0],
        "last_month": months[-1],
        "categories": {k: {"reviews": v["reviews"], "avg_rating": round(v["rating_sum"] / v["reviews"], 3),
                           "verified_share": round(v["verified"] / v["reviews"], 3)}
                       for k, v in sorted(per_category.items())},
    }


def render(data, template_path=TEMPLATE):
    with open(template_path, encoding="utf-8") as handle:
        template = handle.read()
    marker = "/*DASHBOARD_DATA*/null"
    if marker not in template:
        raise ValueError(f"{template_path} has no {marker} placeholder")
    # "</" would end the <script> element early if any text (a product title)
    # contained "</script>".
    payload = json.dumps(data, separators=(",", ":")).replace("</", "<\\/")
    return template.replace(marker, payload)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--from-json", action="store_true", help="re-render from data/dashboard/data.json")
    parser.add_argument("--workgroup", default="rrl")
    parser.add_argument("--region", default="ap-southeast-2")
    parser.add_argument("--out", default=OUT_DIR)
    parser.add_argument("--log", default=DEFAULT_LOG)
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)
    data_path = os.path.join(args.out, "data.json")
    if args.from_json:
        with open(data_path, encoding="utf-8") as handle:
            data = json.load(handle)
    else:
        results, runs = query_all(args.region, args.workgroup, args.log)
        data = {
            "built_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
            "windows": dict(WINDOWS, min_reviews=MIN_REVIEWS),
            "compare_days": COMPARE_DAYS,
            "monthly": results["category_monthly"],
            "daily": results["category_daily"],
            "falling": results["falling_products"],
            "headline": headline(results["category_monthly"]),
            "sales": sale_effects(results["category_daily"]),
            "queries": runs,
        }
        with open(data_path, "w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=1)

    page = os.path.join(args.out, "index.html")
    with open(page, "w", encoding="utf-8") as handle:
        handle.write(render(data))
    scanned = sum(q["data_scanned_bytes"] for q in data["queries"])
    cost = sum(q["cost_usd"] for q in data["queries"])
    print(f"wrote {os.path.relpath(page, ROOT)}  ({len(data['queries'])} queries, "
          f"{scanned:,} bytes scanned, ${cost:.6f})")


if __name__ == "__main__":
    main()
