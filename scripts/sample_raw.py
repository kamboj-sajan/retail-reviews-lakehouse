#!/usr/bin/env python3
"""Save the first N lines of each chosen source file into data/sample/raw/.

Local development and tests run against these small files, never against the
full dataset. The files are laid out exactly like the raw S3 bucket
(reviews/category=<name>/ and meta/category=<name>/), so a job reads the local
sample and the real bucket with the same code.

The sample is the HEAD of each file, not a random sample: it stops the HTTP
download as soon as it has enough lines, so it costs a few MB of traffic
instead of several GB. data/ is git-ignored; the dataset must not be
redistributed.

Standard library only.

Usage:
    python3 scripts/sample_raw.py                 # 20,000 reviews + 5,000 products per category
    python3 scripts/sample_raw.py --reviews 1000 --meta 500
"""
import argparse
import gzip
import json
import os
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG = os.path.join(HERE, "..", "config", "categories.json")
DEFAULT_OUT = os.path.join(HERE, "..", "data", "sample", "raw")
USER_AGENT = "retail-reviews-lakehouse-downloader/1.0"


def save_head(url, destination, line_count):
    """Stream url, decompress on the fly, keep line_count lines, recompress."""
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    written = 0
    with urllib.request.urlopen(request, timeout=120) as response:
        # GzipFile reads from the network stream a block at a time, so only the
        # bytes needed for the first line_count lines are ever downloaded.
        with gzip.GzipFile(fileobj=response) as source, gzip.open(destination, "wb") as target:
            for line in source:
                target.write(line)
                written += 1
                if written >= line_count:
                    break
    return written


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--out", default=DEFAULT_OUT)
    parser.add_argument("--reviews", type=int, default=20000, help="review lines per category")
    parser.add_argument("--meta", type=int, default=5000, help="product lines per category")
    args = parser.parse_args()

    with open(args.config, encoding="utf-8") as handle:
        config = json.load(handle)
    for category in config["categories"]:
        jobs = [
            (f"{config['base_url']}/review_categories/{category}.jsonl.gz",
             os.path.join("reviews", f"category={category}", f"{category}.jsonl.gz"), args.reviews),
            (f"{config['base_url']}/meta_categories/meta_{category}.jsonl.gz",
             os.path.join("meta", f"category={category}", f"meta_{category}.jsonl.gz"), args.meta),
        ]
        for url, relative_path, count in jobs:
            destination = os.path.join(args.out, relative_path)
            os.makedirs(os.path.dirname(destination), exist_ok=True)
            written = save_head(url, destination, count)
            print(f"{relative_path}: {written} lines")


if __name__ == "__main__":
    main()
