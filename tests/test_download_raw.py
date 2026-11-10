"""Tests for scripts/download_raw.py.

No real network and no real AWS: `moto` provides an in-memory fake S3, and the
HTTP source is replaced by a function that slices a bytes object.
"""
import os

import boto3
import pytest
from moto import mock_aws

import download_raw

MB = 1024 * 1024
BUCKET = "test-raw"
KEY = "reviews/category=Video_Games/Video_Games.jsonl.gz"
URL = "https://example.invalid/Video_Games.jsonl.gz"


@pytest.fixture
def s3():
    # Fake credentials so boto3 never looks for (or uses) real ones.
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def fake_source(size):
    """A pretend remote file: (bytes, HEAD result, fetch function, call log)."""
    blob = os.urandom(size)
    calls = []

    def fetch(url, start, end):
        calls.append((start, end))
        return blob[start:end + 1]

    source = {"size": size, "etag": "abc-123", "last_modified": "Fri, 17 Jan 2025 00:36:51 GMT"}
    return blob, source, fetch, calls


def test_plan_parts_covers_every_byte_exactly_once():
    parts = download_raw.plan_parts(total_bytes=25, part_size=10)
    assert parts == [(1, 0, 9), (2, 10, 19), (3, 20, 24)]


def test_plan_parts_exact_multiple_has_no_empty_last_part():
    assert download_raw.plan_parts(total_bytes=20, part_size=10) == [(1, 0, 9), (2, 10, 19)]


def test_plan_parts_small_file_is_one_part():
    assert download_raw.plan_parts(total_bytes=3, part_size=10) == [(1, 0, 2)]


def test_plan_parts_rejects_empty_file():
    with pytest.raises(ValueError):
        download_raw.plan_parts(total_bytes=0, part_size=10)


def test_source_files_builds_hive_style_keys():
    config = {"base_url": "https://host/raw", "categories": ["Video_Games"]}
    files = download_raw.source_files(config)
    assert [f["key"] for f in files] == [
        "reviews/category=Video_Games/Video_Games.jsonl.gz",
        "meta/category=Video_Games/meta_Video_Games.jsonl.gz",
    ]
    assert files[0]["url"] == "https://host/raw/review_categories/Video_Games.jsonl.gz"
    assert files[1]["url"] == "https://host/raw/meta_categories/meta_Video_Games.jsonl.gz"


def test_upload_is_byte_identical(s3):
    # 12 MB with 5 MB parts gives 3 parts (5 MB is S3's minimum part size).
    blob, source, fetch, _ = fake_source(12 * MB)
    copied = download_raw.upload_file(s3, BUCKET, KEY, URL, source, fetch=fetch, part_size=5 * MB, workers=3)
    assert copied == len(blob)
    stored = s3.get_object(Bucket=BUCKET, Key=KEY)
    assert stored["Body"].read() == blob
    assert stored["Metadata"]["source-etag"] == "abc-123"


def test_second_run_is_skipped(s3):
    _, source, fetch, calls = fake_source(6 * MB)
    assert not download_raw.already_uploaded(s3, BUCKET, KEY, source)
    download_raw.upload_file(s3, BUCKET, KEY, URL, source, fetch=fetch, part_size=5 * MB, workers=2)
    assert download_raw.already_uploaded(s3, BUCKET, KEY, source)


def test_changed_source_is_not_skipped(s3):
    _, source, fetch, _ = fake_source(6 * MB)
    download_raw.upload_file(s3, BUCKET, KEY, URL, source, fetch=fetch, part_size=5 * MB, workers=2)
    newer = dict(source, etag="different-version")
    assert not download_raw.already_uploaded(s3, BUCKET, KEY, newer)


def test_failed_upload_leaves_nothing_behind(s3):
    blob, source, _, _ = fake_source(12 * MB)

    def flaky_fetch(url, start, end):
        if start >= 5 * MB:
            raise RuntimeError("network down")
        return blob[start:end + 1]

    with pytest.raises(RuntimeError, match="network down"):
        download_raw.upload_file(s3, BUCKET, KEY, URL, source, fetch=flaky_fetch, part_size=5 * MB, workers=1)

    # No finished object, and no half-finished upload still costing money.
    assert "Contents" not in s3.list_objects_v2(Bucket=BUCKET)
    assert s3.list_multipart_uploads(Bucket=BUCKET).get("Uploads", []) == []
