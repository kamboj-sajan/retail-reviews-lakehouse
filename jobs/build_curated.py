"""Job: cleaned reviews + products -> curated tables.

Two modes:
- full (no --month): rebuild every month;
- monthly (--month YYYY-MM): read only that month of cleaned reviews and
  replace only that month's curated partitions. Every curated table is
  partitioned by month, so other months are untouched.

Local run:
    PYTHONPATH=src python jobs/build_curated.py --cleaned_path data/local/cleaned --curated_path data/local/curated
"""
import argparse

from pyspark.sql import functions as F

from lakehouse.metrics import JobRun, path_bytes
from lakehouse.session import build_spark
from lakehouse.storage import month_argument, write_partitioned
from lakehouse.transforms import category_daily, category_monthly, enrich_reviews, product_monthly

AGGREGATES = {
    "category_monthly": category_monthly,
    "category_daily": category_daily,
    "product_monthly": product_monthly,
}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--cleaned_path", required=True, help="root of the cleaned layer")
    parser.add_argument("--curated_path", required=True, help="root of the curated layer")
    parser.add_argument("--metrics_path", help="where to write this run's metrics record")
    parser.add_argument("--month", type=month_argument, help="process only this month (YYYY-MM)")
    args, _ = parser.parse_known_args(argv)
    month = args.month

    spark = build_spark("build_curated")
    run = JobRun(spark, "build_curated", args.metrics_path)

    def this_run(df):
        """The rows this run is responsible for: one month, or everything."""
        return df.where(F.col("review_month") == month) if month else df

    # Parquet stores each column separately, so leaving out title and text
    # here means those (large) columns are never read from S3 at all. The
    # month filter is on a partition column, so Spark reads only that
    # month's folders (partition pruning).
    reviews = this_run(spark.read.parquet(f"{args.cleaned_path}/reviews")).drop("title", "text")
    products = spark.read.parquet(f"{args.cleaned_path}/products")

    enriched = run.observe(
        enrich_reviews(reviews, products),
        enriched_rows=F.count(F.lit(1)),
        reviews_without_product=F.count(F.when(~F.col("has_product"), 1)),
    )
    enriched_path = f"{args.curated_path}/reviews_enriched"
    write_partitioned(enriched, enriched_path, ["category", "review_month"])

    # The aggregates read the enriched table back from Parquet instead of
    # recomputing the join three times. Reading a few columns of Parquet is
    # cheaper than a join, and it needs no memory for caching.
    written = this_run(spark.read.parquet(enriched_path))
    run.record(month=month or "all", reviews_enriched_rows=written.count())
    for name, build in AGGREGATES.items():
        target = f"{args.curated_path}/{name}"
        # Partitioned by month because a month is the unit of an incremental
        # load: re-running March replaces only the March folder.
        write_partitioned(build(written), target, ["review_month"])
        run.record(**{f"{name}_rows": this_run(spark.read.parquet(target)).count()})

    run.record(output_bytes=path_bytes(spark, args.curated_path))
    run.save()


if __name__ == "__main__":
    main()
