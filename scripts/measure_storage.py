#!/usr/bin/env python3
"""Measure what every layer of the lake actually stores in S3: files and bytes.

Lists each bucket (ListObjectsV2, no data read) and adds up object sizes per
area, and per category where the area is split by category. The headline
comparison is the one the phase 1 codec choice was based on, now on the full
data rather than the sample: the downloaded gzipped JSON reviews against the
cleaned zstd Parquet reviews, category by category.

The result is printed and saved to benchmarks/storage.json.

Usage:
    python scripts/measure_storage.py --raw <bucket> --cleaned <bucket> --curated <bucket> \\
        --quarantine <bucket> --artifacts <bucket>
"""
import argparse
import collections
import datetime
import json
import os
import re

import boto3

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
OUT = os.path.join(ROOT, "benchmarks", "storage.json")

# (bucket role, area) -> how deep the area name goes in the key
AREAS = {
    "raw": ["reviews", "meta", "feed/reviews", "landing/reviews"],
    "cleaned": ["reviews", "products"],
    "curated": ["reviews_enriched", "category_monthly", "category_daily", "product_monthly"],
    "quarantine": ["reviews", "products"],
    "artifacts": ["warehouse", "metrics", "spark-logs", "athena-results", "glue"],
}
CATEGORY = re.compile(r"category=([^/]+)/")


def area_of(role, key):
    for area in AREAS[role]:
        if key.startswith(area + "/"):
            return area
    return "(other)"


def measure(s3, buckets):
    totals = collections.defaultdict(lambda: {"files": 0, "bytes": 0})
    for role, bucket in buckets.items():
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith("/") or os.path.basename(key).startswith(("_", ".")):
                    continue  # folder markers and _SUCCESS files are not data
                area = area_of(role, key)
                match = CATEGORY.search(key)
                for group in [(role, area, "all"), (role, area, match.group(1) if match else None)]:
                    if group[2] is None:
                        continue
                    totals[group]["files"] += 1
                    totals[group]["bytes"] += obj["Size"]
    return totals


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    for role in AREAS:
        parser.add_argument(f"--{role}", required=True)
    parser.add_argument("--region", default="ap-southeast-2")
    args = parser.parse_args()

    totals = measure(boto3.client("s3", region_name=args.region), {r: getattr(args, r) for r in AREAS})
    rows = [{"bucket": r, "area": a, "category": c, **v} for (r, a, c), v in sorted(totals.items())]

    print(f"{'bucket':11} {'area':18} {'category':16} {'files':>7} {'bytes':>15} {'GB':>7}")
    for row in rows:
        print(f"{row['bucket']:11} {row['area']:18} {row['category']:16} {row['files']:>7,} {row['bytes']:>15,} "
              f"{row['bytes'] / 1e9:>7.2f}")

    raw = {r["category"]: r for r in rows if (r["bucket"], r["area"]) == ("raw", "reviews")}
    parquet = {r["category"]: r for r in rows if (r["bucket"], r["area"]) == ("cleaned", "reviews")}
    print("\nreviews: downloaded gzipped JSON vs cleaned zstd Parquet")
    print(f"{'category':16} {'gzip JSON':>15} {'Parquet':>15} {'Parquet/JSON':>13} {'Parquet files':>14}")
    comparison = []
    for category in sorted(raw):
        if category not in parquet:
            continue
        ratio = parquet[category]["bytes"] / raw[category]["bytes"]
        comparison.append({"category": category, "gzip_json_bytes": raw[category]["bytes"],
                           "parquet_bytes": parquet[category]["bytes"], "ratio": round(ratio, 3),
                           "parquet_files": parquet[category]["files"]})
        print(f"{category:16} {raw[category]['bytes']:>15,} {parquet[category]['bytes']:>15,} {ratio:>12.2f}x "
              f"{parquet[category]['files']:>14,}")

    with open(OUT, "w", encoding="utf-8") as handle:
        json.dump({"measured_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
                   "areas": rows, "reviews_json_vs_parquet": comparison}, handle, indent=1)


if __name__ == "__main__":
    main()
