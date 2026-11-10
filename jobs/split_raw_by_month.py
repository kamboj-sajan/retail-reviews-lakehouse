"""Job (run once): raw review files -> one gzipped JSON Lines file per category and month.

Why this job exists
-------------------
Each raw review file is one gzip stream, and gzip cannot be split: a reader
must start at byte 0, so one Spark task reads a whole file. The 6.47 GB
Electronics file takes about 25 minutes that way (docs/decisions.md). The
monthly pipeline must not pay that for every month, so this job pays it once:
it reads every raw line and writes it, unchanged, into a folder for its
category and month:

    <feed_path>/reviews/category=<category>/review_month=<YYYY-MM>/part-*.txt.gz

That output plays the part of the upstream system's monthly exports. The
monthly replay (scripts/replay_month.py) "delivers" one month at a time by
copying that month's folder into the landing area.

The lines are copied as text, not parsed and re-written, so each one stays
byte-for-byte what was downloaded. Only the timestamp is read, to pick the
month.

Local run:
    PYTHONPATH=src python jobs/split_raw_by_month.py --raw_path data/sample/raw --feed_path data/local/feed
"""
import argparse

from pyspark.sql import functions as F

from lakehouse.metrics import JobRun, path_bytes, path_count
from lakehouse.session import build_spark
from lakehouse.storage import parse_categories, raw_paths, read_raw_review_lines


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--raw_path", required=True, help="root of the raw layer")
    parser.add_argument("--feed_path", help="where to write the monthly files (default: <raw_path>/feed)")
    parser.add_argument("--metrics_path", help="where to write this run's metrics record")
    parser.add_argument("--categories", help="comma-separated; default: every category")
    args, _ = parser.parse_known_args(argv)
    categories = parse_categories(args.categories)
    feed = f"{args.feed_path or args.raw_path + '/feed'}/reviews"

    spark = build_spark("split_raw_by_month")
    # With no shuffle, each task writes straight from the file it is reading.
    # Spark normally sorts a task's rows by the partition columns before a
    # partitioned write, so that it has one output file open at a time. For a
    # 30 GB decompressed file that sort spills to disk. Instead, keep up to
    # 1,000 files open at once (one per month; there are about 300) and write
    # each line as it is read. Planned writes would add the sort back, so
    # they are turned off for this job.
    spark.conf.set("spark.sql.optimizer.plannedWrite.enabled", "false")
    spark.conf.set("spark.sql.maxConcurrentOutputFileWriters", "1000")
    run = JobRun(spark, "split_raw_by_month", args.metrics_path)

    timestamp_ms = F.expr("try_cast(get_json_object(value, '$.timestamp') AS BIGINT)")
    lines = run.observe(
        read_raw_review_lines(spark, args.raw_path, categories)
        .withColumn("review_month", F.date_format(F.timestamp_millis(timestamp_ms), "yyyy-MM")),
        raw_lines=F.count(F.lit(1)),
        lines_without_month=F.count(F.when(F.col("review_month").isNull(), 1)),
    )
    (
        lines.select("value", "category", "review_month")
        .write.mode("overwrite")  # dynamic partition overwrite: a re-run replaces, never appends
        .partitionBy("category", "review_month")
        .option("compression", "gzip")
        .text(feed)
    )

    run.record(
        categories=",".join(categories) if categories else "all",
        input_bytes=sum(path_bytes(spark, p) for p in raw_paths(f"{args.raw_path}/reviews", categories)),
        output_bytes=sum(path_bytes(spark, p) for p in raw_paths(feed, categories)),
        month_folders=path_count(spark, f"{feed}/category=*/review_month=*"),
        output_files=path_count(spark, f"{feed}/category=*/review_month=*/*.gz"),
    )
    run.save()


if __name__ == "__main__":
    main()
