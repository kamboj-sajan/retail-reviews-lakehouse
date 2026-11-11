"""Job: curated reviews + cleaned products -> the Redshift star schema.

Runs after the quality gate (check_quality), so only a month that passed
every check reaches the warehouse. Two modes:

- monthly (--month YYYY-MM): replace that month of fact_review and that
  month's days in dim_date;
- full (no --month): replace all of fact_review, dim_date (every day from
  the first review to the last) and dim_product.

dim_product is loaded only in full mode: product metadata comes from
clean_products, which runs only in a backfill, never in a monthly load.

Steps (lakehouse/warehouse.py explains why each is done this way):
1. Spark writes Parquet load files under --staging_path whose columns match
   the tables in sql/warehouse/schema.sql exactly, measuring as it writes:
   rows, rating sum, and the longest value of every text column.
2. If a value is longer than its VARCHAR column, stop and name it.
3. One Redshift transaction (Data API) replaces the rows from those files.
4. Queries in Redshift check that it now holds exactly what Spark wrote, and
   that every review has its date and product rows. A mismatch fails the run.

Without --workgroup, only step 1 runs and the load SQL is printed: that is
how it runs locally, where there is no Redshift.

Local run:
    PYTHONPATH=src python jobs/load_warehouse.py --cleaned_path data/local/cleaned \
        --curated_path data/local/curated --staging_path data/local/warehouse
"""
import argparse
import json

from pyspark.sql import functions as F

from lakehouse.metrics import JobRun
from lakehouse.session import build_spark
from lakehouse.storage import month_argument, read_table
from lakehouse.warehouse import (VARCHAR_BYTES, RedshiftData, check_query, compare, date_filter, dim_date,
                                 dim_product, fact_review, files_prefix, load_transaction, month_bounds,
                                 month_filter)


class WarehouseLoadFailed(Exception):
    pass


def write_load_files(run, df, table, path):
    """Write df as snappy Parquet at path, measuring rows and text lengths as it goes.

    Snappy rather than the lake's zstd: these files exist only to be read by
    COPY, and snappy is the codec every Parquet reader supports.
    """
    measures = {f"{table}_rows": F.count(F.lit(1))}
    measures.update({f"{table}_max_bytes_{c}": F.max(F.octet_length(c)) for c in VARCHAR_BYTES[table]})
    if table == "fact_review":
        measures["fact_review_rating_sum"] = F.coalesce(F.sum("rating"), F.lit(0))
    run.observe(df, **measures).write.mode("overwrite").option("compression", "snappy").parquet(path)


def too_long(counts, tables):
    """Every text value longer than its VARCHAR column, as readable strings."""
    problems = []
    for table in tables:
        for column, limit in VARCHAR_BYTES[table].items():
            longest = counts.get(f"{table}_max_bytes_{column}")
            if longest is not None and longest > limit:
                problems.append(f"{table}.{column}: {longest} bytes, column holds {limit}")
    return problems


def main(argv=None, data_client=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cleaned_path", required=True)
    parser.add_argument("--curated_path", required=True)
    parser.add_argument("--staging_path", required=True, help="where the load files are written")
    parser.add_argument("--metrics_path")
    parser.add_argument("--month", type=month_argument, help="load only this month (YYYY-MM)")
    parser.add_argument("--workgroup", help="Redshift Serverless workgroup; without it, only write the files")
    parser.add_argument("--database", default="reviews")
    parser.add_argument("--copy_role_arn", help="IAM role Redshift uses to read the load files")
    parser.add_argument("--region", default="ap-southeast-2")
    args, _ = parser.parse_known_args(argv)
    month = args.month
    load_name = month or "full"

    spark = build_spark("load_warehouse")
    run = JobRun(spark, "load_warehouse", args.metrics_path)

    enriched = read_table(spark, f"{args.curated_path}/reviews_enriched", month)
    if month:
        first_day, last_day = month_bounds(month)
    else:
        first_day, last_day = enriched.agg(F.min("review_date"), F.max("review_date")).first()

    # (table, the rows, which existing rows they replace)
    tables = [
        ("dim_date", dim_date(spark, first_day, last_day), date_filter(first_day, last_day) if month else None),
        ("fact_review", fact_review(enriched), month_filter(month) if month else None),
    ]
    if not month:
        tables.insert(1, ("dim_product", dim_product(spark.read.parquet(f"{args.cleaned_path}/products")), None))

    loads = []
    for table, rows, replace_where in tables:
        prefix = files_prefix(args.staging_path, table, load_name)
        write_load_files(run, rows, table, prefix)
        loads.append((table, prefix, replace_where))
    counts = run.observed()
    run.record(month=month or "all", first_day=first_day.isoformat(), last_day=last_day.isoformat())

    problems = too_long(counts, [t for t, _, _ in loads])
    if problems:
        run.record(loaded=False)
        run.save()
        raise WarehouseLoadFailed("values too long for their Redshift columns: " + "; ".join(problems))

    statements = load_transaction(loads, args.copy_role_arn or "<copy role arn>")
    if not args.workgroup:
        print("No --workgroup: load files written, nothing loaded. The load would run:")
        print(";\n".join(statements) + ";")
        run.record(loaded=False)
        run.save()
        return

    if data_client is None:
        import boto3
        data_client = boto3.client("redshift-data", region_name=args.region)
    redshift = RedshiftData(data_client, args.workgroup, args.database)
    loaded = redshift.run(statements)
    measured = redshift.query(check_query(month))[0]
    expected = {"fact_rows": counts["fact_review_rows"], "rating_sum": counts["fact_review_rating_sum"]}
    checks = compare(measured, expected)
    for name, got, want, passed in checks:
        print(f"{name:58} {got:>12} {want:>12}  {'pass' if passed else 'FAIL'}")

    run.record(loaded=True, load_statement_id=loaded["Id"],
               redshift_load_seconds=round(loaded.get("Duration", 0) / 1e9, 1),
               checks_json=json.dumps([{"name": n, "measured": g, "expected": w, "passed": p}
                                       for n, g, w, p in checks]),
               **{f"redshift_{k}": v for k, v in measured.items()})
    run.save()
    failed = [c for c in checks if not c[3]]
    if failed:
        raise WarehouseLoadFailed("; ".join(f"{n}: measured {g}, expected {w}" for n, g, w, _ in failed))


if __name__ == "__main__":
    main()
