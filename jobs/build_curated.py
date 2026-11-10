"""Job: cleaned reviews -> curated aggregate tables.

Local run:
    PYTHONPATH=src python jobs/build_curated.py --cleaned_path data/local/cleaned --curated_path data/local/curated
"""
import argparse

from lakehouse.session import build_spark
from lakehouse.storage import write_partitioned
from lakehouse.transforms import category_daily, category_monthly, product_monthly

TABLES = {
    "category_monthly": category_monthly,
    "category_daily": category_daily,
    "product_monthly": product_monthly,
}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cleaned_path", required=True, help="root of the cleaned layer")
    parser.add_argument("--curated_path", required=True, help="root of the curated layer")
    args, _ = parser.parse_known_args(argv)

    spark = build_spark("build_curated")
    # Only the columns the aggregates use are read. Parquet stores each column
    # separately, so the large `text` column is never loaded from disk here.
    reviews = spark.read.parquet(f"{args.cleaned_path}/reviews").select(
        "category", "parent_asin", "rating", "verified_purchase", "review_date", "review_month"
    )
    # All three tables are built from the same input. cache() keeps it in
    # memory after the first table so the files are not read three times.
    reviews.cache()

    for name, build in TABLES.items():
        target = f"{args.curated_path}/{name}"
        # Partitioned by month because a month is the unit of an incremental
        # load: re-running March replaces only the March folder.
        write_partitioned(build(reviews), target, ["review_month"])
        print(f"build_curated: {spark.read.parquet(target).count()} rows in {target}")

    reviews.unpersist()


if __name__ == "__main__":
    main()
