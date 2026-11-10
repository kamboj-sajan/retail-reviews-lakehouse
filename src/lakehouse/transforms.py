"""Transformations between the layers.

Every function takes a DataFrame and returns a DataFrame. None of them reads
or writes files, which is what makes them easy to test with a few hand-made
rows (tests/test_transforms.py).
"""
from pyspark.sql import functions as F

from lakehouse.schemas import CORRUPT_COLUMN

# ---------------------------------------------------------------------------
# raw -> cleaned
# ---------------------------------------------------------------------------


def clean_reviews(raw):
    """Typed review rows with a real timestamp and the partition columns.

    Not done here yet: de-duplication (phase 2) and sending bad rows to
    quarantine (phase 5). Lines that are not valid JSON are dropped for now.
    """
    review_ts = F.timestamp_millis(F.col("timestamp"))  # milliseconds -> timestamp
    rating = F.col("rating")
    return (
        raw.where(F.col(CORRUPT_COLUMN).isNull())
        .select(
            "user_id",
            "asin",
            "parent_asin",
            # Star ratings are whole numbers, so store an integer. A value
            # like 4.5 becomes NULL rather than being silently rounded to 4;
            # the quality checks in phase 5 catch NULL ratings.
            F.when(rating == F.floor(rating), rating.cast("int")).alias("rating"),
            # try_cast, not cast: a plain cast of a number too big for an int
            # silently wraps around to a different number. try_cast gives NULL.
            F.expr("try_cast(helpful_vote as int)").alias("helpful_vote"),
            "verified_purchase",
            "title",
            "text",
            # size(NULL) is -1 in Spark, so handle a missing array explicitly.
            F.when(F.col("images").isNull(), F.lit(0)).otherwise(F.size("images")).alias("image_count"),
            review_ts.alias("review_ts"),
            F.to_date(review_ts).alias("review_date"),
            # Partition columns. review_month is text like "2023-03": it sorts
            # correctly as text, so BETWEEN '2022-11' AND '2023-01' works.
            "category",
            F.date_format(review_ts, "yyyy-MM").alias("review_month"),
        )
    )


def clean_products(raw):
    """One typed row per product record, keeping the columns reporting needs.

    Left in the raw layer only: images, videos, description, features,
    bought_together. They can be added later by re-running from raw.
    """
    return (
        raw.where(F.col(CORRUPT_COLUMN).isNull())
        .select(
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
    )


# ---------------------------------------------------------------------------
# cleaned -> curated
#
# Each table stores review_count and rating_sum, not only the average.
# Counts and sums can be added up again later (months into years, products
# into categories). Averages cannot: the average of monthly averages is wrong
# whenever the months have different numbers of reviews.
# ---------------------------------------------------------------------------


def _rating_measures():
    return [
        F.count(F.lit(1)).alias("review_count"),
        F.sum("rating").alias("rating_sum"),
        F.round(F.avg("rating"), 4).alias("avg_rating"),
        F.sum(F.col("verified_purchase").cast("int")).alias("verified_count"),
    ]


def category_monthly(cleaned_reviews):
    """One row per category and month. Feeds the rating-trend visual."""
    return cleaned_reviews.groupBy("category", "review_month").agg(*_rating_measures())


def category_daily(cleaned_reviews):
    """One row per category and day. Feeds the sale-period volume visual."""
    return cleaned_reviews.groupBy("category", "review_date", "review_month").agg(*_rating_measures())


def product_monthly(cleaned_reviews):
    """One row per product and month. Feeds the falling-ratings visual."""
    return cleaned_reviews.groupBy("category", "parent_asin", "review_month").agg(*_rating_measures())
