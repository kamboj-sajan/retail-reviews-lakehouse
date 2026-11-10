"""Transformations between the layers.

Every function takes a DataFrame and returns a DataFrame. None of them reads
or writes files, which is what makes them easy to test with a few hand-made
rows (tests/test_transforms.py).
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

from lakehouse.schemas import CORRUPT_COLUMN

# The columns that identify one review. docs/decisions.md ("De-duplication
# key") has the measurements behind this choice. The timestamp is part of the
# key, so copies of a review always fall in the same month: de-duplicating one
# month at a time (phase 3) finds every copy without looking at other months.
REVIEW_KEY = ["user_id", "parent_asin", "timestamp"]

PRODUCT_KEY = ["category", "parent_asin"]

# ---------------------------------------------------------------------------
# quarantine rules
#
# A raw row that breaks a rule is not loaded. It goes to the quarantine
# bucket with the reason, so it can be counted, inspected and, once the cause
# is fixed, replayed. Rules are checked in this order; a row gets the first
# reason that applies.
# ---------------------------------------------------------------------------

# 2100-01-01 UTC in milliseconds. A later (or negative) timestamp is a corrupt
# value, and converting it to a date would overflow.
MAX_TIMESTAMP_MS = 4_102_444_800_000

REVIEW_PROBLEMS = [
    "unparseable_json",         # the line is not valid JSON, or a field has the wrong type
    "missing_key",              # user_id, parent_asin or timestamp is missing
    "timestamp_out_of_range",   # before 1970 or after 2099
    "rating_not_whole_number",  # missing, or not 1.0, 2.0, ... (4.5 is not a star rating)
    "rating_out_of_range",      # a whole number, but not 1 to 5 (the real data has two 0s)
    "outside_month",            # monthly runs only: the timestamp is in another month
]
PRODUCT_PROBLEMS = ["unparseable_json", "missing_key"]

RAW_REVIEW_FIELDS = ["user_id", "asin", "parent_asin", "rating", "title", "text",
                     "timestamp", "helpful_vote", "verified_purchase", "images"]
RAW_PRODUCT_FIELDS = ["parent_asin", "title", "main_category", "store", "price",
                      "average_rating", "rating_number", "categories", "details"]


def _month_of(timestamp_ms):
    return F.date_format(F.timestamp_millis(timestamp_ms), "yyyy-MM")


def review_problem(month=None):
    """Column: why a raw review row cannot be loaded, or NULL if it can."""
    ts, rating = F.col("timestamp"), F.col("rating")
    problem = (
        F.when(F.col(CORRUPT_COLUMN).isNotNull(), F.lit("unparseable_json"))
        .when(F.col("user_id").isNull() | F.col("parent_asin").isNull() | ts.isNull(), F.lit("missing_key"))
        .when((ts < 0) | (ts > MAX_TIMESTAMP_MS), F.lit("timestamp_out_of_range"))
        .when(rating.isNull() | (rating != F.round(rating)), F.lit("rating_not_whole_number"))
        .when((rating < 1) | (rating > 5), F.lit("rating_out_of_range"))
    )
    if month:
        # Without this rule, one stray row would make dynamic partition
        # overwrite replace another month with just that row.
        problem = problem.when(_month_of(ts) != F.lit(month), F.lit("outside_month"))
    return problem


def split_reviews(raw, month=None):
    """raw review rows -> (rows to load, rows to quarantine).

    Quarantined rows keep the record as JSON text: the original line when it
    could not be parsed, otherwise the parsed raw fields (before any type
    conversion, so a rating of 4.5 is still visible as 4.5).

    load_month is the quarantine partition: the month being loaded in a
    monthly run, so a re-run of that month replaces exactly its quarantine
    rows; in a backfill, the row's own month, or "unknown" without one.
    """
    classified = raw.withColumn("_problem", review_problem(month))
    loadable = classified.where(F.col("_problem").isNull()).drop("_problem")

    ts = F.col("timestamp")
    if month:
        load_month = F.lit(month)
    else:
        valid_ts = ts.isNotNull() & (ts >= 0) & (ts <= MAX_TIMESTAMP_MS)
        load_month = F.when(valid_ts, _month_of(ts)).otherwise(F.lit("unknown"))
    quarantined = classified.where(F.col("_problem").isNotNull()).select(
        F.col("_problem").alias("reason"),
        F.coalesce(F.col(CORRUPT_COLUMN), F.to_json(F.struct(*RAW_REVIEW_FIELDS))).alias("record"),
        "category",
        load_month.alias("load_month"),
    )
    return loadable, quarantined


def split_products(raw):
    """raw product rows -> (rows to load, rows to quarantine)."""
    problem = (
        F.when(F.col(CORRUPT_COLUMN).isNotNull(), F.lit("unparseable_json"))
        .when(F.col("parent_asin").isNull(), F.lit("missing_key"))
    )
    classified = raw.withColumn("_problem", problem)
    loadable = classified.where(F.col("_problem").isNull()).drop("_problem")
    quarantined = classified.where(F.col("_problem").isNotNull()).select(
        F.col("_problem").alias("reason"),
        F.coalesce(F.col(CORRUPT_COLUMN), F.to_json(F.struct(*RAW_PRODUCT_FIELDS))).alias("record"),
        "category",
    )
    return loadable, quarantined


def count_by_reason(reasons, prefix="quarantined_"):
    """Observation aggregates: one count per quarantine reason, plus the total."""
    counts = {f"{prefix}{r}": F.count(F.when(F.col("reason") == r, 1)) for r in reasons}
    counts[f"{prefix}rows"] = F.count(F.lit(1))
    return counts


# ---------------------------------------------------------------------------
# raw -> cleaned: reviews
# ---------------------------------------------------------------------------


def review_id(user_id, parent_asin, timestamp_ms):
    """A stable 32-character ID computed from the review key.

    The same review always gets the same ID, on every run and every machine.
    That is what lets later loads (Redshift, phase 4) replace a review instead
    of inserting it twice. MD5 is used as a fingerprint here, not for
    security; at 65 million rows the chance of two keys colliding is about
    1 in 10^22.
    """
    parts = [F.coalesce(c.cast("string"), F.lit("")) for c in (user_id, parent_asin, timestamp_ms)]
    return F.md5(F.concat_ws("|", *parts))


def type_reviews(raw):
    """One typed row per parseable raw line: real types, ID, partition columns.

    Lines that are not valid JSON are dropped here; phase 5 sends them to
    quarantine instead.
    """
    review_ts = F.timestamp_millis(F.col("timestamp"))  # milliseconds -> timestamp
    rating = F.col("rating")
    return (
        raw.where(F.col(CORRUPT_COLUMN).isNull())
        .select(
            review_id(F.col("user_id"), F.col("parent_asin"), F.col("timestamp")).alias("review_id"),
            "user_id",
            "asin",
            "parent_asin",
            # Star ratings are whole numbers, so store an integer. A value
            # like 4.5 becomes NULL rather than being rounded to 4. round(),
            # not floor(): floor returns a 64-bit integer, which under Spark 4's
            # ANSI mode raises an error for a huge value instead of comparing.
            F.when(rating == F.round(rating), F.expr("try_cast(rating as int)")).alias("rating"),
            # try_cast, not cast: outside ANSI mode a plain cast of a number
            # too big for an int silently wraps around; under ANSI it fails
            # the job. try_cast gives NULL, which the quality checks can count.
            F.expr("try_cast(helpful_vote as int)").alias("helpful_vote"),
            "verified_purchase",
            "title",
            "text",
            # size(NULL) is -1 (or NULL under ANSI), so handle a missing array.
            F.when(F.col("images").isNull(), F.lit(0)).otherwise(F.size("images")).alias("image_count"),
            # Kept until de-duplication, which needs it, then dropped.
            F.col("timestamp").alias("timestamp"),
            review_ts.alias("review_ts"),
            F.to_date(review_ts).alias("review_date"),
            # Partition columns. review_month is text like "2023-03": it sorts
            # correctly as text, so BETWEEN '2022-11' AND '2023-01' works.
            "category",
            F.date_format(review_ts, "yyyy-MM").alias("review_month"),
        )
    )


def deduplicate_reviews(typed):
    """Keep one row per (category, REVIEW_KEY).

    Which copy is kept must not depend on the order Spark happens to read
    rows in, or two runs over the same input could keep different rows. So
    the copies are sorted on every column and the first is kept:

    - highest helpful_vote first. Vote counts only grow, so when copies of a
      review disagree on votes, the higher count is the later snapshot;
    - then the remaining columns, purely as a tie-breaker.

    Rows with part of the key missing are passed through untouched: grouping
    on NULLs would merge unrelated reviews. Phase 5 quarantines them.
    """
    key = ["category"] + REVIEW_KEY
    complete = F.lit(True)
    for column in key:
        complete = complete & F.col(column).isNotNull()

    order = [F.col("helpful_vote").desc_nulls_last()] + [
        F.col(c).asc_nulls_last()
        for c in ("asin", "rating", "verified_purchase", "title", "text", "image_count")
    ]
    ranked = typed.where(complete).withColumn(
        "_copy", F.row_number().over(Window.partitionBy(*key).orderBy(*order))
    )
    kept = ranked.where(F.col("_copy") == 1).drop("_copy")
    incomplete = typed.where(~complete)
    return kept.unionByName(incomplete).drop("timestamp")


def clean_reviews(raw):
    """raw review lines -> cleaned reviews: typed and de-duplicated."""
    return deduplicate_reviews(type_reviews(raw))


# ---------------------------------------------------------------------------
# raw -> cleaned: products
# ---------------------------------------------------------------------------


def clean_products(raw):
    """One typed row per product per category.

    The full Video_Games file has no repeated parent_asin (docs/decisions.md),
    but a repeat would multiply that product's reviews in every join, so it is
    guarded against anyway. The copy with the most ratings is kept: it is the
    most recent snapshot.

    Left in the raw layer only: images, videos, description, features,
    bought_together. They can be added later by re-running from raw.
    """
    typed = raw.where(F.col(CORRUPT_COLUMN).isNull()).select(
        "parent_asin",
        "title",
        "main_category",
        "store",
        # try_cast returns NULL for text that is not a number.
        F.expr("try_cast(price as double)").alias("price"),
        "average_rating",
        F.expr("try_cast(rating_number as int)").alias("rating_number"),
        "categories",
        F.col("details").alias("details_json"),
        "category",
    )
    order = [F.col("rating_number").desc_nulls_last()] + [
        F.col(c).asc_nulls_last() for c in ("title", "store", "price", "main_category", "details_json")
    ]
    return (
        typed.where(F.col("parent_asin").isNotNull())
        .withColumn("_copy", F.row_number().over(Window.partitionBy(*PRODUCT_KEY).orderBy(*order)))
        .where(F.col("_copy") == 1)
        .drop("_copy")
    )


# ---------------------------------------------------------------------------
# cleaned -> curated
# ---------------------------------------------------------------------------


def enrich_reviews(reviews, products):
    """Reviews joined to their product, without the review text.

    A LEFT join: a review whose product is missing from the metadata is kept,
    with has_product = false, so no review disappears from the totals. The
    join is on category as well as parent_asin because products are
    de-duplicated per category; joining on parent_asin alone could match one
    review to the same product listed in two categories and count it twice.
    """
    product_columns = products.select(
        "category",
        "parent_asin",
        F.lit(True).alias("has_product"),
        F.col("title").alias("product_title"),
        "store",
        "price",
        "main_category",
    )
    return (
        reviews.drop("title", "text")
        .join(product_columns, on=PRODUCT_KEY, how="left")
        .withColumn("has_product", F.coalesce(F.col("has_product"), F.lit(False)))
        .select(
            "review_id", "user_id", "asin", "parent_asin", "rating", "helpful_vote",
            "verified_purchase", "image_count", "review_ts", "review_date",
            "has_product", "product_title", "store", "price", "main_category",
            "category", "review_month",
        )
    )


# Each aggregate stores review_count and rating_sum, not only the average.
# Counts and sums can be added up again later (months into years, products
# into categories). Averages cannot: the average of monthly averages is wrong
# whenever the months have different numbers of reviews.


def _rating_measures():
    return [
        F.count(F.lit(1)).alias("review_count"),
        F.sum("rating").alias("rating_sum"),
        F.round(F.avg("rating"), 4).alias("avg_rating"),
        F.sum(F.col("verified_purchase").cast("int")).alias("verified_count"),
    ]


def category_monthly(enriched):
    """One row per category and month. Feeds the rating-trend visual."""
    return enriched.groupBy("category", "review_month").agg(*_rating_measures())


def category_daily(enriched):
    """One row per category and day. Feeds the sale-period volume visual."""
    return enriched.groupBy("category", "review_date", "review_month").agg(*_rating_measures())


def product_monthly(enriched):
    """One row per product and month. Feeds the falling-ratings visual.

    product_title and store come from the join. After product de-duplication
    each (category, parent_asin) has exactly one title and store, so adding
    them to the grouping does not split any product's rows.
    """
    return enriched.groupBy(
        "category", "parent_asin", "product_title", "store", "review_month"
    ).agg(*_rating_measures())
