#!/usr/bin/env python3
"""Delete a range of months from the cleaned and curated layers (and landing).

Used once, before the incremental replay: the backfill loaded every month, so
to replay months as genuinely new data they are removed first. After the
replay the totals must equal the backfill's again, which checks that monthly
loads and the full load agree.

Deletes, for each month M in the range:
    cleaned:  reviews/category=*/review_month=M/
    curated:  reviews_enriched/category=*/review_month=M/
              category_monthly/review_month=M/  category_daily/...  product_monthly/...
    raw:      landing/reviews/category=*/review_month=M/  and  landing/_arrived/M
The original downloads and raw/feed/ are never touched. Everything deleted
can be rebuilt by replaying the month.

Without --yes this only lists what it would delete.

Usage:
    python scripts/reset_months.py --raw <bucket> --cleaned <bucket> --curated <bucket> --from 2022-10 --to 2023-09
    python scripts/reset_months.py ... --yes
"""
import argparse

import boto3

CLEANED_TABLES = ["reviews/"]
CURATED_TABLES = ["reviews_enriched/", "category_monthly/", "category_daily/", "product_monthly/"]


def months_between(first, last):
    year, month = map(int, first.split("-"))
    while f"{year:04d}-{month:02d}" <= last:
        yield f"{year:04d}-{month:02d}"
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)


def matching(s3, bucket, prefix, months):
    markers = {f"/review_month={m}/" for m in months} | {f"{prefix}review_month={m}/" for m in months}
    found = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if any(marker in key or key.startswith(marker) for marker in markers):
                found.append((key, obj["Size"]))
    return found


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--raw", required=True)
    parser.add_argument("--cleaned", required=True)
    parser.add_argument("--curated", required=True)
    parser.add_argument("--from", dest="first", required=True, help="first month, YYYY-MM")
    parser.add_argument("--to", dest="last", required=True, help="last month, YYYY-MM (inclusive)")
    parser.add_argument("--yes", action="store_true", help="really delete")
    parser.add_argument("--region", default="ap-southeast-2")
    args = parser.parse_args()

    months = list(months_between(args.first, args.last))
    s3 = boto3.client("s3", region_name=args.region)
    targets = [(args.cleaned, t) for t in CLEANED_TABLES] + [(args.curated, t) for t in CURATED_TABLES] + [
        (args.raw, "landing/reviews/")]
    print(f"months {months[0]} to {months[-1]} ({len(months)})")

    plan = []
    for bucket, prefix in targets:
        objects = matching(s3, bucket, prefix, months)
        plan.append((bucket, objects))
        print(f"  s3://{bucket}/{prefix:20} {len(objects):6} objects  {sum(s for _, s in objects) / 1e9:8.3f} GB")
    arrived = [(f"landing/_arrived/{m}", 0) for m in months]
    plan.append((args.raw, arrived))
    print(f"  s3://{args.raw}/landing/_arrived/   up to {len(arrived)} markers")

    if not args.yes:
        print("dry run: nothing deleted (add --yes to delete)")
        return
    for bucket, objects in plan:
        keys = [k for k, _ in objects]
        for i in range(0, len(keys), 1000):  # DeleteObjects takes at most 1,000 keys
            s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]], "Quiet": True})
    print("deleted")


if __name__ == "__main__":
    main()
