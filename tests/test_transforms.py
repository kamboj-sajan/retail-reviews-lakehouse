"""Tests for reading raw JSON and for the layer transformations.

Each test writes or builds a few rows by hand, so the expected answer is known
exactly. No sample data and no AWS access are needed.
"""
import datetime
import glob
import gzip
import json
import os

import random

from lakehouse.metrics import JobRun, path_bytes
from lakehouse.storage import read_raw_products, read_raw_reviews, write_partitioned
from lakehouse.transforms import (
    category_daily,
    category_monthly,
    clean_products,
    clean_reviews,
    enrich_reviews,
    product_monthly,
)
from pyspark.sql import functions as F

# 2023-01-31 23:59:59.999 UTC, one millisecond before February. In India
# (UTC+5:30) the same instant is already 1 February, so this value shows
# whether the month is computed in UTC or in the machine's time zone.
LAST_MS_OF_JANUARY_UTC = 1675209599999

GOOD_REVIEW = {
    "user_id": "u1", "asin": "A1", "parent_asin": "P1", "rating": 5.0,
    "title": "Great", "text": "Works well", "timestamp": LAST_MS_OF_JANUARY_UTC,
    "helpful_vote": 3, "verified_purchase": True,
    "images": [{"small_image_url": "a"}, {"small_image_url": "b"}],
}

GOOD_PRODUCT = {
    "parent_asin": "P1", "title": "Thing", "main_category": "Video Games", "store": "Acme",
    "price": 58.0, "average_rating": 4.5, "rating_number": 10,
    "categories": ["Video Games", "PC"], "details": {"Brand": "Acme", "Rank": {"Games": 5}},
}


def write_jsonl_gz(path, lines):
    """Write text lines to a .jsonl.gz file, creating its folder."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def raw_reviews(spark, tmp_path, reviews, category="Video_Games"):
    """Write dicts as a raw review file in the S3 layout and read it back."""
    lines = [r if isinstance(r, str) else json.dumps(r) for r in reviews]
    write_jsonl_gz(str(tmp_path / "raw" / "reviews" / f"category={category}" / "part.jsonl.gz"), lines)
    return read_raw_reviews(spark, str(tmp_path / "raw"))


def by_user(df):
    return {row["user_id"]: row for row in df.collect()}


# --- reading raw -----------------------------------------------------------


def test_category_comes_from_the_folder_name(spark, tmp_path):
    raw = raw_reviews(spark, tmp_path, [GOOD_REVIEW], category="Toys_and_Games")
    assert [row["category"] for row in raw.collect()] == ["Toys_and_Games"]


def test_unparseable_and_wrongly_typed_lines_are_flagged_not_lost(spark, tmp_path):
    raw = raw_reviews(spark, tmp_path, [
        GOOD_REVIEW,
        '{"user_id": "u2", "asin": ',                    # truncated JSON
        dict(GOOD_REVIEW, user_id="u3", rating="five"),  # text where a number belongs
    ]).cache()  # Spark refuses to query the corrupt column alone on an uncached JSON read

    assert raw.count() == 3
    flagged = raw.where("_corrupt_record IS NOT NULL").count()
    assert flagged == 2


def test_unknown_fields_are_ignored(spark, tmp_path):
    raw = raw_reviews(spark, tmp_path, [dict(GOOD_REVIEW, some_new_field="x")])
    assert "some_new_field" not in raw.columns
    assert len(clean_reviews(raw).collect()) == 1  # the row is good, not flagged


# --- raw -> cleaned reviews ------------------------------------------------


def test_clean_reviews_types(spark, tmp_path):
    cleaned = clean_reviews(raw_reviews(spark, tmp_path, [GOOD_REVIEW]))
    types = dict(cleaned.dtypes)
    assert types["review_id"] == "string"
    assert types["rating"] == "int"
    assert types["helpful_vote"] == "int"
    assert types["verified_purchase"] == "boolean"
    assert types["review_ts"] == "timestamp"
    assert types["review_date"] == "date"
    assert types["review_month"] == "string"
    assert "images" not in types and "timestamp" not in types


def test_timestamp_is_milliseconds_and_month_is_computed_in_utc(spark, tmp_path):
    row = clean_reviews(raw_reviews(spark, tmp_path, [GOOD_REVIEW])).collect()[0]
    assert row["review_date"] == datetime.date(2023, 1, 31)
    assert row["review_month"] == "2023-01"


def test_clean_reviews_edge_cases(spark, tmp_path):
    cleaned = by_user(clean_reviews(raw_reviews(spark, tmp_path, [
        GOOD_REVIEW,
        dict(GOOD_REVIEW, user_id="half_star", rating=4.5),
        dict(GOOD_REVIEW, user_id="huge_star", rating=1e20),
        dict(GOOD_REVIEW, user_id="no_images", images=None),
        dict(GOOD_REVIEW, user_id="huge_votes", helpful_vote=99999999999),
        dict(GOOD_REVIEW, user_id="no_time", timestamp=None),
        '{"user_id": "broken", ',
    ])))

    assert cleaned["u1"]["rating"] == 5 and cleaned["u1"]["image_count"] == 2
    assert cleaned["half_star"]["rating"] is None          # not rounded to 4
    assert cleaned["huge_star"]["rating"] is None          # no ANSI overflow error
    assert cleaned["no_images"]["image_count"] == 0        # not -1
    assert cleaned["huge_votes"]["helpful_vote"] is None   # not wrapped to 1215752191
    assert cleaned["no_time"]["review_month"] is None
    assert "broken" not in cleaned and len(cleaned) == 6   # the unparseable line is dropped


# --- de-duplication --------------------------------------------------------


def clean_rows(spark, tmp_path, reviews):
    return clean_reviews(raw_reviews(spark, tmp_path, reviews)).collect()


def test_exact_copies_collapse_to_one(spark, tmp_path):
    rows = clean_rows(spark, tmp_path, [GOOD_REVIEW] * 11)  # the largest group seen in Video_Games
    assert len(rows) == 1


def test_same_review_on_two_variants_counts_once(spark, tmp_path):
    rows = clean_rows(spark, tmp_path, [GOOD_REVIEW, dict(GOOD_REVIEW, asin="A2")])
    assert len(rows) == 1
    assert rows[0]["asin"] == "A1"  # tie broken on asin, ascending


def test_copy_with_most_helpful_votes_is_kept(spark, tmp_path):
    rows = clean_rows(spark, tmp_path, [dict(GOOD_REVIEW, helpful_vote=v) for v in (3, 5, 4)])
    assert [r["helpful_vote"] for r in rows] == [5]


def test_choice_does_not_depend_on_input_order(spark, tmp_path):
    copies = [dict(GOOD_REVIEW, helpful_vote=1, title=t) for t in ("b", "a", "c")]
    first = clean_rows(spark, tmp_path / "one", copies)
    random.Random(7).shuffle(copies)
    second = clean_rows(spark, tmp_path / "two", copies)
    assert first == second and first[0]["title"] == "a"


def test_different_reviews_are_not_merged(spark, tmp_path):
    rows = clean_rows(spark, tmp_path, [
        GOOD_REVIEW,
        dict(GOOD_REVIEW, timestamp=GOOD_REVIEW["timestamp"] + 1),  # same user and product, later
        dict(GOOD_REVIEW, parent_asin="P2"),                         # same user and time, other product
        dict(GOOD_REVIEW, user_id="u2"),                             # other user
    ])
    assert len(rows) == 4
    assert len({r["review_id"] for r in rows}) == 4


def test_rows_missing_part_of_the_key_are_not_merged(spark, tmp_path):
    rows = clean_rows(spark, tmp_path, [dict(GOOD_REVIEW, timestamp=None)] * 2)
    assert len(rows) == 2  # left for the quality checks, not guessed to be one review


def test_review_id_is_stable_and_comes_from_the_key(spark, tmp_path):
    one = clean_rows(spark, tmp_path / "one", [GOOD_REVIEW])[0]["review_id"]
    # Same key, different non-key fields: the same review, so the same ID.
    two = clean_rows(spark, tmp_path / "two", [dict(GOOD_REVIEW, text="edited", asin="A9")])[0]["review_id"]
    assert one == two
    assert len(one) == 32 and int(one, 16) >= 0


# --- raw -> cleaned products -----------------------------------------------


def test_clean_products(spark, tmp_path):
    lines = [json.dumps(p) for p in [
        GOOD_PRODUCT,
        dict(GOOD_PRODUCT, parent_asin="P2", price="19.99"),       # number written as text
        dict(GOOD_PRODUCT, parent_asin="P3", price="from 12.99"),  # not a number
        dict(GOOD_PRODUCT, parent_asin="P4", price=None),
    ]]
    write_jsonl_gz(str(tmp_path / "raw" / "meta" / "category=Video_Games" / "meta.jsonl.gz"), lines)
    rows = {r["parent_asin"]: r for r in clean_products(read_raw_products(spark, str(tmp_path / "raw"))).collect()}

    assert rows["P1"]["price"] == 58.0
    assert rows["P2"]["price"] == 19.99
    assert rows["P3"]["price"] is None
    assert rows["P4"]["price"] is None
    assert rows["P1"]["categories"] == ["Video Games", "PC"]
    assert rows["P1"]["category"] == "Video_Games"
    # The nested object is kept whole, as JSON text, in one column.
    assert json.loads(rows["P1"]["details_json"]) == {"Brand": "Acme", "Rank": {"Games": 5}}


def write_products(tmp_path, by_category):
    for category, products in by_category.items():
        write_jsonl_gz(str(tmp_path / "raw" / "meta" / f"category={category}" / "meta.jsonl.gz"),
                       [json.dumps(p) for p in products])


def test_products_deduplicated_per_category(spark, tmp_path):
    write_products(tmp_path, {
        "Video_Games": [dict(GOOD_PRODUCT, rating_number=10, title="old"),
                        dict(GOOD_PRODUCT, rating_number=12, title="new"),
                        dict(GOOD_PRODUCT, parent_asin=None)],
        "Electronics": [GOOD_PRODUCT],  # same parent_asin, another category: a separate row
    })
    rows = clean_products(read_raw_products(spark, str(tmp_path / "raw"))).collect()
    by_category = {r["category"]: r for r in rows}
    assert len(rows) == 2
    assert by_category["Video_Games"]["title"] == "new"  # the copy with more ratings
    assert by_category["Electronics"]["parent_asin"] == "P1"


# --- join ------------------------------------------------------------------


def test_enrich_keeps_every_review_and_matches_within_category(spark, tmp_path):
    reviews = clean_reviews(raw_reviews(spark, tmp_path, [
        GOOD_REVIEW,
        dict(GOOD_REVIEW, user_id="u2", parent_asin="NOT_IN_METADATA"),
    ]))
    write_products(tmp_path, {
        "Video_Games": [dict(GOOD_PRODUCT, title="Game", store="Acme")],
        "Electronics": [dict(GOOD_PRODUCT, title="Same ID, other category")],
    })
    products = clean_products(read_raw_products(spark, str(tmp_path / "raw")))
    rows = {r["user_id"]: r for r in enrich_reviews(reviews, products).collect()}

    assert len(rows) == 2  # not 3: the Electronics listing of P1 must not match
    assert rows["u1"]["has_product"] is True
    assert rows["u1"]["product_title"] == "Game" and rows["u1"]["store"] == "Acme"
    assert rows["u2"]["has_product"] is False and rows["u2"]["product_title"] is None
    assert "text" not in enrich_reviews(reviews, products).columns


# --- writing partitions ----------------------------------------------------


def month_review(user, month_start_ms, rating=5.0, parent="P1"):
    return dict(GOOD_REVIEW, user_id=user, timestamp=month_start_ms, rating=rating, parent_asin=parent)


JAN = 1672531200000  # 2023-01-01 00:00:00 UTC
FEB = 1675209600000  # 2023-02-01 00:00:00 UTC


def test_write_creates_one_file_per_partition_folder(spark, tmp_path):
    cleaned = clean_reviews(raw_reviews(spark, tmp_path, [
        month_review("a", JAN), month_review("b", JAN), month_review("c", FEB),
    ]))
    target = str(tmp_path / "cleaned" / "reviews")
    write_partitioned(cleaned, target, ["category", "review_month"])

    january = glob.glob(f"{target}/category=Video_Games/review_month=2023-01/*.parquet")
    february = glob.glob(f"{target}/category=Video_Games/review_month=2023-02/*.parquet")
    assert len(january) == 1 and len(february) == 1

    back = spark.read.parquet(target)
    assert back.count() == 3
    assert dict(back.dtypes)["review_month"] == "string"  # not guessed to be a date


def test_rerun_does_not_duplicate_and_leaves_other_months_alone(spark, tmp_path):
    target = str(tmp_path / "cleaned" / "reviews")
    both = clean_reviews(raw_reviews(spark, tmp_path, [
        month_review("a", JAN), month_review("b", JAN), month_review("c", FEB),
    ]))
    write_partitioned(both, target, ["category", "review_month"])
    february_file_before = glob.glob(f"{target}/category=Video_Games/review_month=2023-02/*.parquet")

    # Run January again, twice. Only January's folder should be replaced.
    january_only = both.where("review_month = '2023-01'")
    write_partitioned(january_only, target, ["category", "review_month"])
    write_partitioned(january_only, target, ["category", "review_month"])

    assert spark.read.parquet(target).count() == 3
    assert glob.glob(f"{target}/category=Video_Games/review_month=2023-02/*.parquet") == february_file_before


# --- cleaned -> curated ----------------------------------------------------


def test_curated_aggregates(spark, tmp_path):
    reviews = clean_reviews(raw_reviews(spark, tmp_path, [
        month_review("a", JAN, rating=5.0, parent="P1"),
        month_review("b", JAN, rating=2.0, parent="P1"),
        dict(month_review("c", JAN, rating=4.0, parent="P2"), verified_purchase=False),
        month_review("d", FEB, rating=1.0, parent="P1"),
    ]))
    write_products(tmp_path, {"Video_Games": [GOOD_PRODUCT]})  # P1 only; P2 has no metadata
    cleaned = enrich_reviews(reviews, clean_products(read_raw_products(spark, str(tmp_path / "raw"))))

    monthly = {r["review_month"]: r for r in category_monthly(cleaned).collect()}
    assert monthly["2023-01"]["review_count"] == 3
    assert monthly["2023-01"]["rating_sum"] == 11
    assert monthly["2023-01"]["avg_rating"] == 3.6667
    assert monthly["2023-01"]["verified_count"] == 2
    assert monthly["2023-02"]["review_count"] == 1

    products = {(r["parent_asin"], r["review_month"]): r for r in product_monthly(cleaned).collect()}
    assert products[("P1", "2023-01")]["review_count"] == 2
    assert products[("P1", "2023-01")]["avg_rating"] == 3.5
    assert products[("P1", "2023-01")]["product_title"] == "Thing"
    assert products[("P2", "2023-01")]["review_count"] == 1
    assert products[("P2", "2023-01")]["product_title"] is None  # kept, with no metadata
    assert products[("P1", "2023-02")]["rating_sum"] == 1

    daily = category_daily(cleaned).collect()
    assert sorted((str(r["review_date"]), r["review_count"]) for r in daily) == [("2023-01-01", 3), ("2023-02-01", 1)]


# --- metrics ---------------------------------------------------------------


def test_job_run_records_counts_from_the_write_itself(spark, tmp_path):
    run = JobRun(spark, "test_job", str(tmp_path / "metrics"))
    df = run.observe(spark.range(10), rows=F.count(F.lit(1)), evens=F.count(F.when(F.col("id") % 2 == 0, 1)))
    df.write.parquet(str(tmp_path / "out"))
    run.record(output_bytes=path_bytes(spark, str(tmp_path / "out")))
    record = run.save()

    assert record["rows"] == 10 and record["evens"] == 5
    assert record["output_bytes"] > 0
    saved = spark.read.json(str(tmp_path / "metrics")).collect()
    assert len(saved) == 1 and saved[0]["rows"] == 10 and saved[0]["job_name"] == "test_job"
