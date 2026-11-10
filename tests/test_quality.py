"""Phase 5: quarantine rules, and the quality checks that fail a run.

The check tests build one real month from the local sample exactly as the
pipeline does (split, deliver, clean, curate), confirm every check passes,
then break the data in specific ways and confirm the right check fails.
"""
import glob
import gzip
import json
import os
import shutil

import pytest

import build_curated
import check_quality
import clean_products
import clean_reviews
import split_raw_by_month
from lakehouse.quality import check_month
from lakehouse.storage import read_raw_reviews
from lakehouse.transforms import split_reviews

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
SAMPLE_RAW = os.path.join(ROOT, "data", "sample", "raw")
needs_sample = pytest.mark.skipif(
    not glob.glob(os.path.join(SAMPLE_RAW, "reviews", "category=*", "*.jsonl.gz")),
    reason="no local sample; run `make sample`",
)

JAN = 1672531200000  # 2023-01-01 00:00:00 UTC
GOOD = {"user_id": "u", "asin": "A1", "parent_asin": "P1", "rating": 5.0, "title": "t", "text": "x",
        "timestamp": JAN, "helpful_vote": 0, "verified_purchase": True, "images": []}


def write_lines(path, lines):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


# --- the rules ------------------------------------------------------------------


def test_each_rule_sends_its_rows_to_quarantine_with_a_reason(spark, tmp_path):
    cases = {
        "ok": GOOD,
        "rating_zero": dict(GOOD, user_id="z", rating=0.0),              # as found twice in real Electronics data
        "rating_seven": dict(GOOD, user_id="s", rating=7.0),
        "half_star": dict(GOOD, user_id="h", rating=4.5),
        "no_rating": dict(GOOD, user_id="n", rating=None),
        "no_user": dict(GOOD, user_id=None),
        "no_product": dict(GOOD, user_id="p", parent_asin=None),
        "no_time": dict(GOOD, user_id="t", timestamp=None),
        "negative_time": dict(GOOD, user_id="m", timestamp=-5),
        "far_future": dict(GOOD, user_id="f", timestamp=9_000_000_000_000_000),
        "february": dict(GOOD, user_id="feb", timestamp=JAN + 31 * 86_400_000),
    }
    lines = [json.dumps(v) for v in cases.values()] + ['{"user_id": "broken", ', '{"rating": "five"}']
    write_lines(str(tmp_path / "raw" / "reviews" / "category=Video_Games" / "part.jsonl.gz"), lines)
    raw = read_raw_reviews(spark, str(tmp_path / "raw"))

    loadable, quarantined = split_reviews(raw, month="2023-01")
    reasons = sorted(r["reason"] for r in quarantined.collect())
    assert [r["user_id"] for r in loadable.collect()] == ["u"]
    assert reasons == sorted([
        "rating_out_of_range", "rating_out_of_range", "rating_not_whole_number", "rating_not_whole_number",
        "missing_key", "missing_key", "missing_key", "timestamp_out_of_range", "timestamp_out_of_range",
        "outside_month", "unparseable_json", "unparseable_json",
    ])

    # The record keeps the raw value, before type conversion: the half star is still 4.5.
    records = {json.loads(r["record"]).get("user_id"): json.loads(r["record"])
               for r in quarantined.collect() if r["reason"] != "unparseable_json"}
    assert records["h"]["rating"] == 4.5 and records["z"]["rating"] == 0.0
    assert all(r["load_month"] == "2023-01" for r in quarantined.collect())
    # Backfill mode has no "outside month": February is simply loadable there.
    loadable_backfill, _ = split_reviews(raw)
    assert sorted(r["user_id"] for r in loadable_backfill.collect()) == ["feb", "u"]


def test_rerun_without_bad_rows_clears_that_months_quarantine(spark, tmp_path):
    landing = tmp_path / "landing"
    folder = landing / "reviews" / "category=Video_Games" / "review_month=2023-01"
    args = ["--raw_path", str(landing), "--cleaned_path", str(tmp_path / "cleaned"),
            "--quarantine_path", str(tmp_path / "quarantine"), "--month", "2023-01"]

    write_lines(str(folder / "part.txt.gz"), [json.dumps(GOOD), json.dumps(dict(GOOD, user_id="bad", rating=0.0))])
    clean_reviews.main(args)
    assert spark.read.parquet(str(tmp_path / "quarantine" / "reviews")).count() == 1

    shutil.rmtree(folder)
    write_lines(str(folder / "part.txt.gz"), [json.dumps(GOOD)])  # the source was fixed and re-delivered
    clean_reviews.main(args)
    assert not glob.glob(str(tmp_path / "quarantine" / "reviews" / "category=*" / "load_month=2023-01" / "*.parquet"))


# --- the checks -------------------------------------------------------------------


@pytest.fixture
def loaded_month(spark, tmp_path):
    """One real month from the sample, loaded the way the state machine loads it."""
    paths = {name: str(tmp_path / name) for name in ("feed", "landing", "cleaned", "curated", "quarantine", "metrics")}
    split_raw_by_month.main(["--raw_path", SAMPLE_RAW, "--feed_path", paths["feed"]])
    clean_products.main(["--raw_path", SAMPLE_RAW, "--cleaned_path", paths["cleaned"],
                         "--quarantine_path", paths["quarantine"]])
    # The busiest month in the sample.
    folders = glob.glob(f"{paths['feed']}/reviews/category=*/review_month=*")
    by_month = {}
    for folder in folders:
        month = folder.rsplit("=", 1)[1]
        by_month[month] = by_month.get(month, 0) + os.path.getsize(glob.glob(f"{folder}/*.gz")[0])
    month = max(by_month, key=by_month.get)
    for folder in glob.glob(f"{paths['feed']}/reviews/category=*/review_month={month}"):
        category = os.path.basename(os.path.dirname(folder))
        shutil.copytree(folder, f"{paths['landing']}/reviews/{category}/review_month={month}")
    paths["month"] = month
    return paths


def run_month(paths):
    common = ["--month", paths["month"], "--metrics_path", paths["metrics"]]
    clean_reviews.main(["--raw_path", paths["landing"], "--cleaned_path", paths["cleaned"],
                        "--quarantine_path", paths["quarantine"], *common])
    build_curated.main(["--cleaned_path", paths["cleaned"], "--curated_path", paths["curated"], *common])


def checks_for(spark, paths, max_rate=0.01):
    checks, measured = check_month(spark, paths["month"], paths["landing"], paths["cleaned"], paths["curated"],
                                   paths["quarantine"], paths["metrics"], max_rate)
    return {c.name: c for c in checks}, measured


def quality_args(paths):
    return ["--month", paths["month"], "--raw_path", paths["landing"], "--cleaned_path", paths["cleaned"],
            "--curated_path", paths["curated"], "--quarantine_path", paths["quarantine"],
            "--metrics_path", paths["metrics"]]


@needs_sample
def test_a_correctly_loaded_month_passes_every_check(spark, loaded_month):
    run_month(loaded_month)
    checks, measured = checks_for(spark, loaded_month)
    assert [name for name, c in checks.items() if not c.passed] == []
    assert len(checks) >= 20
    assert measured["landing_lines"] == (measured["cleaned_rows"] + measured["quarantined_rows"]
                                         + measured["duplicate_rows_removed"])
    check_quality.main(quality_args(loaded_month))  # does not raise


@needs_sample
def test_a_few_bad_rows_are_quarantined_and_the_month_still_passes(spark, loaded_month):
    folder = glob.glob(f"{loaded_month['landing']}/reviews/category=*/review_month={loaded_month['month']}")[0]
    write_lines(f"{folder}/injected.txt.gz", ['{"broken', json.dumps(dict(GOOD, rating=0.0))])
    run_month(loaded_month)
    checks, measured = checks_for(spark, loaded_month)
    assert measured["quarantined_rows"] == 2
    assert all(c.passed for c in checks.values())


@needs_sample
def test_too_many_bad_rows_fail_the_quarantine_rate_check(spark, loaded_month):
    folder = glob.glob(f"{loaded_month['landing']}/reviews/category=*/review_month={loaded_month['month']}")[0]
    write_lines(f"{folder}/injected.txt.gz", ['{"broken'] * 500)
    run_month(loaded_month)
    checks, _ = checks_for(spark, loaded_month)
    assert not checks["quarantine rate"].passed
    assert [n for n, c in checks.items() if not c.passed] == ["quarantine rate"]
    with pytest.raises(check_quality.QualityCheckFailed, match="quarantine rate"):
        check_quality.main(quality_args(loaded_month))


@needs_sample
def test_rows_lost_between_layers_fail_reconciliation(spark, loaded_month):
    run_month(loaded_month)
    # Simulate a partial write: one category's enriched file for the month disappears.
    victim = glob.glob(f"{loaded_month['curated']}/reviews_enriched/category=*/review_month={loaded_month['month']}")[0]
    shutil.rmtree(victim)
    checks, _ = checks_for(spark, loaded_month)
    assert not checks["enriched rows = cleaned rows"].passed


@needs_sample
def test_a_duplicate_slipped_into_cleaned_fails_the_duplicate_check(spark, loaded_month):
    run_month(loaded_month)
    table = f"{loaded_month['cleaned']}/reviews"
    month_rows = spark.read.parquet(table).where(f"review_month = '{loaded_month['month']}'")
    month_rows.limit(1).write.mode("append").partitionBy("category", "review_month").parquet(table)
    checks, _ = checks_for(spark, loaded_month)
    assert not checks["cleaned: no duplicate review_id"].passed
    assert not checks["cleaned: no duplicate review key"].passed
