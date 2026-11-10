"""Data quality checks for one loaded month. Any failure fails the pipeline run.

Each check compares a measured value with what it must be, and the report
lists every check, passed or not, so a failed run says exactly what is wrong.
Three kinds of check:

1. Reconciliation: nothing is lost or invented between layers.
       landing lines   = cleaned rows + quarantined rows + duplicate copies removed
       cleaned rows    = enriched rows
       enriched rows   = sum of review_count in each aggregate table
   The landing, cleaned, quarantine and curated counts are measured here,
   independently of the job that wrote them. Only "duplicate copies removed"
   is taken from clean_reviews' own metrics record.
2. Rules on the cleaned data: no missing key columns, no duplicate reviews,
   every rating from 1 to 5. Quarantine should make these impossible; the
   checks prove it did.
3. A quarantine-rate limit. Quarantining a few bad rows is normal. A month
   where many rows fail means something upstream broke, and loading the rest
   would silently publish a partial month.
"""
from dataclasses import asdict, dataclass

from pyspark.sql import functions as F

from lakehouse.storage import raw_paths, read_table

CLEANED_KEY_COLUMNS = ["review_id", "user_id", "parent_asin", "rating", "review_ts", "review_date",
                       "category", "review_month"]
ENRICHED_KEY_COLUMNS = ["review_id", "parent_asin", "review_ts", "has_product", "category", "review_month"]


@dataclass
class Check:
    name: str
    measured: float
    expected: str
    passed: bool


def equal(name, measured, expected):
    return Check(name, measured, f"= {expected}", measured == expected)


def at_most(name, measured, limit):
    return Check(name, measured, f"<= {limit}", measured <= limit)


def latest_metrics(spark, metrics_path, job, month):
    """The most recent metrics record that job wrote for this month."""
    try:
        records = spark.read.json(f"{metrics_path}/job_name={job}")
    except Exception:  # no record at all yet
        return None
    if "month" not in records.columns:
        return None
    rows = records.where(F.col("month") == month).orderBy(F.col("started_utc").desc()).limit(1).collect()
    return rows[0].asDict() if rows else None


def check_month(spark, month, landing_path, cleaned_path, curated_path, quarantine_path, metrics_path,
                max_quarantine_rate=0.01):
    """Run every check for one month. Returns (checks, measurements)."""
    landing_lines = spark.read.text(raw_paths(f"{landing_path}/reviews", month=month)).count()
    quarantine = spark.read.option("basePath", f"{quarantine_path}/reviews")
    try:
        quarantined = quarantine.parquet(f"{quarantine_path}/reviews/category=*/load_month={month}").count()
    except Exception:  # no folder: nothing was quarantined this month
        quarantined = 0

    cleaned = read_table(spark, f"{cleaned_path}/reviews", month).drop("title", "text").cache()
    enriched = read_table(spark, f"{curated_path}/reviews_enriched", month)

    c = cleaned.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("review_id").alias("distinct_ids"),
        F.countDistinct("category", "user_id", "parent_asin", "review_ts").alias("distinct_keys"),
        F.count(F.when(~F.col("rating").between(1, 5), 1)).alias("bad_ratings"),
        *[F.count(F.when(F.col(k).isNull(), 1)).alias(f"null_{k}") for k in CLEANED_KEY_COLUMNS],
    ).first().asDict()
    e = enriched.agg(
        F.count(F.lit(1)).alias("rows"),
        *[F.count(F.when(F.col(k).isNull(), 1)).alias(f"null_{k}") for k in ENRICHED_KEY_COLUMNS],
    ).first().asDict()
    aggregate_totals = {
        table: read_table(spark, f"{curated_path}/{table}", month, by_category=False)
        .agg(F.coalesce(F.sum("review_count"), F.lit(0))).first()[0]
        for table in ("category_monthly", "category_daily", "product_monthly")
    }
    cleaned.unpersist()

    clean_run = latest_metrics(spark, metrics_path, "clean_reviews", month) or {}
    duplicates_removed = clean_run.get("duplicate_rows_removed", -1)

    checks = [
        Check("month has data", landing_lines, "> 0", landing_lines > 0),
        equal("clean_reviews read every landing line", clean_run.get("raw_lines", -1), landing_lines),
        equal("landing = cleaned + quarantined + duplicates removed",
              c["rows"] + quarantined + duplicates_removed, landing_lines),
        equal("enriched rows = cleaned rows", e["rows"], c["rows"]),
        *[equal(f"{table}: sum of review_count = enriched rows", total, e["rows"])
          for table, total in aggregate_totals.items()],
        *[equal(f"cleaned: no NULL {k}", c[f"null_{k}"], 0) for k in CLEANED_KEY_COLUMNS],
        *[equal(f"enriched: no NULL {k}", e[f"null_{k}"], 0) for k in ENRICHED_KEY_COLUMNS],
        equal("cleaned: no duplicate review_id", c["rows"] - c["distinct_ids"], 0),
        equal("cleaned: no duplicate review key", c["rows"] - c["distinct_keys"], 0),
        equal("cleaned: every rating is 1 to 5", c["bad_ratings"], 0),
        at_most("quarantine rate", round(quarantined / landing_lines, 6) if landing_lines else 0.0,
                max_quarantine_rate),
    ]
    measurements = {
        "landing_lines": landing_lines,
        "cleaned_rows": c["rows"],
        "quarantined_rows": quarantined,
        "duplicate_rows_removed": duplicates_removed,
        "enriched_rows": e["rows"],
        **{f"{t}_review_count": v for t, v in aggregate_totals.items()},
    }
    return checks, measurements


def report(checks):
    lines = [f"{'check':58} {'measured':>12}  {'expected':>10}  result"]
    for check in checks:
        lines.append(f"{check.name:58} {check.measured:>12}  {check.expected:>10}  {'pass' if check.passed else 'FAIL'}")
    return "\n".join(lines)


def as_records(checks):
    return [asdict(c) for c in checks]
