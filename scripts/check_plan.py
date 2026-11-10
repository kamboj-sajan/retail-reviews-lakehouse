#!/usr/bin/env python3
"""Refuse a Terraform plan that would delete or replace anything.

Reads `terraform show -json <planfile>` on stdin. Exits 1, listing the
resources, if any action is "delete" (which includes replacements), unless
ALLOW_DESTROY=1 is set.

Why: on 2026-11-10 a network outage during refresh made Terraform report
three live S3 buckets as deleted, and plan to destroy 21 resources and
recreate them. That plan happened to be incomplete, so it could not be
applied, but a plan that is complete and wrong would have been. `make
tf-apply` runs this check between plan and apply.
"""
import json
import os
import sys

plan = json.load(sys.stdin)
deleting = [
    f"{change['address']} ({'replace' if 'create' in change['change']['actions'] else 'delete'})"
    for change in plan.get("resource_changes", [])
    if "delete" in change["change"]["actions"]
]
counts = {}
for change in plan.get("resource_changes", []):
    for action in change["change"]["actions"]:
        counts[action] = counts.get(action, 0) + 1
print("plan actions:", ", ".join(f"{k} {v}" for k, v in sorted(counts.items()) if k != "no-op") or "none")

if deleting and os.environ.get("ALLOW_DESTROY") != "1":
    print(f"\nREFUSED: this plan deletes or replaces {len(deleting)} resource(s):", file=sys.stderr)
    for line in deleting:
        print(f"  {line}", file=sys.stderr)
    print("If that is intended, run again with ALLOW_DESTROY=1.", file=sys.stderr)
    sys.exit(1)
