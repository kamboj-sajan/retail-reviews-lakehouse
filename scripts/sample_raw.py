#!/usr/bin/env python3
"""Save the first N lines of each chosen source file into data/sample/.

Local development and tests run against these small files, never against the
full dataset. The sample is the HEAD of each file, not a random sample: it
stops the HTTP download as soon as it has enough lines, so it costs a few MB of
traffic instead of several GB. data/ is git-ignored; the dataset must not be
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
DEFAULT_OUT = os.path.join(HERE, "..", "data", "sample")
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
    os.makedirs(args.out, exist_ok=True)

    for category in config["categories"]:
        jobs = [
            (f"{config['base_url']}/review_categories/{category}.jsonl.gz", f"{category}.jsonl.gz", args.reviews),
            (f"{config['base_url']}/meta_categories/meta_{category}.jsonl.gz", f"meta_{category}.jsonl.gz", args.meta),
        ]
        for url, name, count in jobs:
            written = save_head(url, os.path.join(args.out, name), count)
            print(f"{name}: {written} lines")


if __name__ == "__main__":
    main()
