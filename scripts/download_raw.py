#!/usr/bin/env python3
"""Copy the chosen Amazon Reviews 2023 files into the raw S3 bucket, byte for byte.

How it works
------------
Each source file is several GB, and this laptop does not have the disk space to
hold them. So nothing is written to local disk. Instead:

1. One HTTP HEAD request per file reads its size.
2. The file is cut into fixed-size byte ranges ("parts").
3. A few worker threads each fetch one part with an HTTP Range request and
   upload it with S3's multipart upload API. S3 stitches the parts together.

Because every part is an independent Range request, a network error only costs
one part (it is retried), not the whole multi-GB transfer.

Memory use is roughly workers x part size (4 x 64 MB = 256 MB by default).

Idempotency: a file is skipped when the object already in S3 has the same size
and the same source ETag, so re-running the script uploads nothing twice.

Usage
-----
    # Show what would be downloaded and how big it is (no AWS access needed):
    python3 scripts/download_raw.py --dry-run

    # Real run (needs AWS credentials and boto3):
    python3 scripts/download_raw.py --bucket <raw-bucket-name>
"""
import argparse
import concurrent.futures
import datetime
import json
import os
import sys
import time
import urllib.error
import urllib.request

MB = 1024 * 1024
DEFAULT_PART_MB = 64   # S3 allows 5 MB to 5 GB per part and at most 10,000 parts.
DEFAULT_WORKERS = 4
USER_AGENT = "retail-reviews-lakehouse-downloader/1.0"
DEFAULT_CONFIG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config", "categories.json")


def plan_parts(total_bytes, part_size):
    """Split [0, total_bytes) into parts.

    Returns a list of (part_number, first_byte, last_byte). Part numbers start
    at 1 because S3 numbers parts from 1. last_byte is inclusive because that
    is how the HTTP Range header counts.
    """
    if total_bytes <= 0:
        raise ValueError("total_bytes must be positive")
    parts = []
    start = 0
    number = 1
    while start < total_bytes:
        end = min(start + part_size, total_bytes) - 1
        parts.append((number, start, end))
        start = end + 1
        number += 1
    return parts


def source_files(config):
    """List every file to copy: one review file and one metadata file per category.

    The S3 key uses a Hive-style `category=<name>/` folder. Review records do
    not contain their category (checked in docs/schema.md), so the path is the
    only place that information lives. Spark and Athena both turn
    `category=Electronics` in a path into a `category` column automatically.
    """
    files = []
    for category in config["categories"]:
        files.append({
            "kind": "reviews",
            "category": category,
            "url": f"{config['base_url']}/review_categories/{category}.jsonl.gz",
            "key": f"reviews/category={category}/{category}.jsonl.gz",
        })
        files.append({
            "kind": "meta",
            "category": category,
            "url": f"{config['base_url']}/meta_categories/meta_{category}.jsonl.gz",
            "key": f"meta/category={category}/meta_{category}.jsonl.gz",
        })
    return files


def head(url):
    """Ask the server for a file's size and version without downloading it."""
    request = urllib.request.Request(url, method="HEAD", headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=60) as response:
        if response.headers.get("Accept-Ranges", "").lower() != "bytes":
            raise RuntimeError(f"{url} does not support Range requests; cannot download in parts")
        return {
            "size": int(response.headers["Content-Length"]),
            "etag": (response.headers.get("ETag") or "").strip('"'),
            "last_modified": response.headers.get("Last-Modified") or "",
        }


def fetch_range(url, start, end, attempts=5):
    """Download bytes start..end (inclusive) of url, retrying on network errors."""
    expected = end - start + 1
    for attempt in range(1, attempts + 1):
        try:
            request = urllib.request.Request(
                url, headers={"Range": f"bytes={start}-{end}", "User-Agent": USER_AGENT}
            )
            with urllib.request.urlopen(request, timeout=120) as response:
                # 206 = Partial Content. A 200 would mean the server ignored the
                # Range header and is sending the whole file.
                if response.status != 206:
                    raise RuntimeError(f"expected HTTP 206, got {response.status}")
                data = response.read()
            if len(data) != expected:
                raise RuntimeError(f"expected {expected} bytes, got {len(data)}")
            return data
        except (urllib.error.URLError, TimeoutError, ConnectionError, RuntimeError) as error:
            if attempt == attempts:
                raise
            wait = 2 ** attempt  # 2, 4, 8, 16 seconds
            print(f"    retry {attempt}/{attempts - 1} for bytes {start}-{end}: {error} (waiting {wait}s)", flush=True)
            time.sleep(wait)


def already_uploaded(s3, bucket, key, source):
    """True when S3 already holds this exact version of the source file."""
    from botocore.exceptions import ClientError
    try:
        existing = s3.head_object(Bucket=bucket, Key=key)
    except ClientError as error:
        if error.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise
    return (
        existing["ContentLength"] == source["size"]
        and existing.get("Metadata", {}).get("source-etag", "") == source["etag"]
    )


def upload_file(s3, bucket, key, url, source, fetch=fetch_range, part_size=DEFAULT_PART_MB * MB, workers=DEFAULT_WORKERS):
    """Copy one source file into S3 with a multipart upload. Returns bytes copied.

    `fetch` is a parameter so the tests can replace the network with a fake.
    """
    parts = plan_parts(source["size"], part_size)
    upload = s3.create_multipart_upload(
        Bucket=bucket,
        Key=key,
        ContentType="application/gzip",
        # Stored with the object so that a later run can tell whether the
        # source file has changed (see already_uploaded).
        Metadata={
            "source-url": url,
            "source-etag": source["etag"],
            "source-last-modified": source["last_modified"],
        },
    )
    upload_id = upload["UploadId"]

    def copy_part(part):
        number, start, end = part
        data = fetch(url, start, end)
        response = s3.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=number, Body=data)
        return {"PartNumber": number, "ETag": response["ETag"]}

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=workers)
    try:
        # Queue every part. Only `workers` of them run at once, and a part's
        # bytes exist in memory only while its own task is running.
        futures = [pool.submit(copy_part, part) for part in parts]
        done = []
        for future in concurrent.futures.as_completed(futures):
            done.append(future.result())  # re-raises here if that part failed
            if len(done) % 10 == 0 or len(done) == len(parts):
                print(f"    {len(done)}/{len(parts)} parts", flush=True)
        pool.shutdown()
        s3.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": sorted(done, key=lambda p: p["PartNumber"])},
        )
    except BaseException:
        # Drop the parts still waiting in the queue, and wait for the ones in
        # flight so that none of them uploads after the abort below.
        pool.shutdown(wait=True, cancel_futures=True)
        # Parts of an unfinished multipart upload are invisible in the bucket
        # but still billed as storage. Always clean up on failure or Ctrl-C.
        s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        raise

    stored = s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
    if stored != source["size"]:
        raise RuntimeError(f"size mismatch for {key}: source {source['size']} bytes, S3 {stored} bytes")
    return stored


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bucket", help="raw-layer S3 bucket name (terraform output: raw_bucket)")
    parser.add_argument("--config", default=DEFAULT_CONFIG, help="path to categories.json")
    parser.add_argument("--only", action="append", help="limit to this category (repeatable)")
    parser.add_argument("--dry-run", action="store_true", help="only list files and sizes; no AWS calls")
    parser.add_argument("--force", action="store_true", help="re-upload even when S3 already has the file")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--part-mb", type=int, default=DEFAULT_PART_MB)
    args = parser.parse_args()

    with open(args.config, encoding="utf-8") as handle:
        config = json.load(handle)
    if args.only:
        unknown = set(args.only) - set(config["categories"])
        if unknown:
            sys.exit(f"not in {args.config}: {sorted(unknown)}")
        config["categories"] = args.only

    files = source_files(config)
    for item in files:
        item["source"] = head(item["url"])

    if args.dry_run:
        total = 0
        print(f"{'kind':8} {'category':18} {'bytes':>14} {'GB':>7}  s3 key")
        for item in files:
            size = item["source"]["size"]
            total += size
            print(f"{item['kind']:8} {item['category']:18} {size:>14} {size / 1e9:>7.2f}  {item['key']}")
        print(f"{'total':27} {total:>14} {total / 1e9:>7.2f}")
        return

    if not args.bucket:
        sys.exit("--bucket is required unless --dry-run is given")

    import boto3  # imported here so --dry-run works without boto3 installed
    s3 = boto3.client("s3")

    manifest = {"started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(), "bucket": args.bucket, "files": []}
    for item in files:
        source = item["source"]
        entry = {"url": item["url"], "key": item["key"], "bytes": source["size"],
                 "source_etag": source["etag"], "source_last_modified": source["last_modified"]}
        if not args.force and already_uploaded(s3, args.bucket, item["key"], source):
            print(f"skip   {item['key']} (already in S3, same size and source ETag)")
            entry.update(status="skipped", seconds=0.0)
        else:
            print(f"upload {item['key']} ({source['size'] / 1e9:.2f} GB)")
            began = time.monotonic()
            upload_file(s3, args.bucket, item["key"], item["url"], source,
                        part_size=args.part_mb * MB, workers=args.workers)
            entry.update(status="uploaded", seconds=round(time.monotonic() - began, 1))
            print(f"    done in {entry['seconds']}s")
        manifest["files"].append(entry)

    # A record of exactly what was copied and when. Benchmarks in phase 7 read
    # raw sizes from here instead of trusting numbers typed by hand.
    manifest["finished_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    manifest_key = f"_manifests/download-{stamp}.json"
    s3.put_object(Bucket=args.bucket, Key=manifest_key, Body=json.dumps(manifest, indent=2).encode(),
                  ContentType="application/json")
    print(f"manifest written to s3://{args.bucket}/{manifest_key}")


if __name__ == "__main__":
    main()
