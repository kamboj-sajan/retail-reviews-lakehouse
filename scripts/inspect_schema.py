#!/usr/bin/env python3
"""Profile the real fields of gzipped JSON Lines files.

Why this exists: the project rule is "check the real schema from the files
before writing any code". This script reads sample files and reports, for every
top-level field, which JSON types actually occur, how often the field is null
or missing, and one example value. docs/schema.md is this script's output.

Standard library only, so it runs with any Python 3.9+ and no virtualenv.

Usage:
    python3 scripts/inspect_schema.py data/sample/*.jsonl.gz
    python3 scripts/inspect_schema.py --markdown --no-examples data/sample/*.jsonl.gz
"""
import argparse
import collections
import datetime
import gzip
import json
import os
import sys


def json_type(value):
    """Name a value's JSON type; for lists, include the element types."""
    if value is None:
        return "null"
    if isinstance(value, bool):  # must come before int: bool is a subclass of int
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "string"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        inner = sorted({json_type(v) for v in value})
        # Square brackets, not <>, so Markdown does not read them as HTML tags.
        return "array[" + "/".join(inner) + "]" if inner else "array[empty]"
    return type(value).__name__


def shorten(value, limit=70):
    text = json.dumps(value, ensure_ascii=False)
    return text if len(text) <= limit else text[: limit - 3] + "..."


def profile(path):
    """Read one .jsonl.gz file and collect per-field statistics."""
    rows = 0
    bad_lines = 0
    types = collections.defaultdict(collections.Counter)  # field -> type -> count
    example = {}                                          # field -> first non-null value
    object_keys = collections.defaultdict(set)            # field -> keys seen in nested objects
    ratings = collections.Counter()
    ts_min = ts_max = None
    review_keys = collections.Counter()                   # (user_id, asin, timestamp) -> count
    asin_differs_from_parent = 0

    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                bad_lines += 1
                continue
            rows += 1
            for field, value in record.items():
                types[field][json_type(value)] += 1
                if value is not None and value != "" and value != [] and value != {}:
                    example.setdefault(field, value)
                if isinstance(value, dict):
                    object_keys[field].update(value.keys())

            # Checks that only apply to review files (they have a timestamp).
            if "timestamp" in record and "user_id" in record:
                ts = record.get("timestamp")
                if isinstance(ts, (int, float)):
                    ts_min = ts if ts_min is None else min(ts_min, ts)
                    ts_max = ts if ts_max is None else max(ts_max, ts)
                ratings[record.get("rating")] += 1
                review_keys[(record.get("user_id"), record.get("asin"), ts)] += 1
                if record.get("asin") != record.get("parent_asin"):
                    asin_differs_from_parent += 1

    return {
        "path": path,
        "rows": rows,
        "bad_lines": bad_lines,
        "types": types,
        "example": example,
        "object_keys": object_keys,
        "ratings": ratings,
        "ts_min": ts_min,
        "ts_max": ts_max,
        "duplicate_key_rows": sum(n - 1 for n in review_keys.values() if n > 1),
        "is_review_file": bool(review_keys),
        "asin_differs_from_parent": asin_differs_from_parent,
    }


def ms_to_date(ms):
    """Review timestamps are Unix epoch MILLISECONDS, so divide by 1000."""
    return datetime.datetime.fromtimestamp(ms / 1000, tz=datetime.timezone.utc).strftime("%Y-%m-%d")


def render(stats, markdown, examples=True):
    out = []
    name = os.path.basename(stats["path"])
    rows = stats["rows"]
    out.append(("### " if markdown else "== ") + name)
    out.append("")
    out.append(f"Rows read: {rows}. Lines that were not valid JSON: {stats['bad_lines']}.")
    out.append("")
    out.append("| Field | Types seen (count) | Missing |" + (" Example |" if examples else ""))
    out.append("|---|---|---|" + ("---|" if examples else ""))
    for field in sorted(stats["types"]):
        counter = stats["types"][field]
        seen = ", ".join(f"{t} ({n})" for t, n in counter.most_common())
        missing = rows - sum(counter.values())
        row = f"| `{field}` | {seen} | {missing} |"
        if examples:
            sample = shorten(stats["example"].get(field, None)).replace("|", "\\|")
            row += f" `{sample}` |"
        out.append(row)
    out.append("")
    for field, keys in sorted(stats["object_keys"].items()):
        preview = ", ".join(sorted(keys)[:8])
        out.append(f"- `{field}` is a nested object with {len(keys)} distinct keys across the sample (first few: {preview}).")
    if stats["is_review_file"]:
        dist = ", ".join(f"{k}: {v}" for k, v in sorted(stats["ratings"].items(), key=lambda kv: str(kv[0])))
        out.append(f"- Rating values: {dist}.")
        if stats["ts_min"] is not None:
            out.append(
                f"- `timestamp` range: {stats['ts_min']} to {stats['ts_max']} "
                f"(as dates: {ms_to_date(stats['ts_min'])} to {ms_to_date(stats['ts_max'])})."
            )
        out.append(f"- Rows that repeat an earlier (user_id, asin, timestamp): {stats['duplicate_key_rows']}.")
        out.append(f"- Rows where `asin` differs from `parent_asin`: {stats['asin_differs_from_parent']}.")
    out.append("")
    return "\n".join(out)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("files", nargs="+", help=".jsonl.gz files to profile")
    parser.add_argument("--markdown", action="store_true", help="emit Markdown headings")
    parser.add_argument("--no-examples", action="store_true",
                        help="leave out example values (used for the committed doc, so no review text or user IDs end up in the repo)")
    args = parser.parse_args()
    for path in args.files:
        sys.stdout.write(render(profile(path), args.markdown, examples=not args.no_examples))
        sys.stdout.write("\n")


if __name__ == "__main__":
    main()
