"""Job: run the data quality checks for one month; fail if any check fails.

Runs after build_curated in the monthly-load state machine. A failure here
fails the Glue run, which fails the state machine's CheckQuality step: the
alert email goes out and nothing after this step runs (the Redshift load, in
phase 4). The checks themselves are in src/lakehouse/quality.py.

Local run:
    PYTHONPATH=src python jobs/check_quality.py --month 2022-11 --raw_path data/local/landing \
        --cleaned_path data/local/cleaned --curated_path data/local/curated \
        --quarantine_path data/local/quarantine --metrics_path data/local/metrics
"""
import argparse
import json

from lakehouse.metrics import JobRun
from lakehouse.quality import as_records, check_month, report
from lakehouse.session import build_spark
from lakehouse.storage import month_argument


class QualityCheckFailed(Exception):
    pass


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--month", type=month_argument, required=True)
    parser.add_argument("--raw_path", required=True, help="root of the landing area the month was loaded from")
    parser.add_argument("--cleaned_path", required=True)
    parser.add_argument("--curated_path", required=True)
    parser.add_argument("--quarantine_path", required=True)
    parser.add_argument("--metrics_path", required=True)
    parser.add_argument("--max_quarantine_rate", type=float, default=0.01,
                        help="fail if more than this share of the month's lines is quarantined (default 0.01 = 1%%)")
    args, _ = parser.parse_known_args(argv)

    spark = build_spark("check_quality")
    run = JobRun(spark, "check_quality", args.metrics_path)
    checks, measurements = check_month(
        spark, args.month, args.raw_path, args.cleaned_path, args.curated_path,
        args.quarantine_path, args.metrics_path, args.max_quarantine_rate,
    )
    failed = [c for c in checks if not c.passed]
    print(report(checks))
    run.record(month=args.month, checks_run=len(checks), checks_failed=len(failed),
               max_quarantine_rate=args.max_quarantine_rate, checks_json=json.dumps(as_records(checks)),
               **measurements)
    run.save()

    if failed:
        summary = "; ".join(f"{c.name}: measured {c.measured}, expected {c.expected}" for c in failed)
        raise QualityCheckFailed(f"{len(failed)} of {len(checks)} quality checks failed for {args.month}: {summary}")


if __name__ == "__main__":
    main()
