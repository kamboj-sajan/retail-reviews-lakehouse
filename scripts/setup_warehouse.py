#!/usr/bin/env python3
"""Create the warehouse schema and the load job's database role. Safe to re-run.

Runs, as the Redshift admin user (password from Secrets Manager, never seen
here: the Data API reads the secret itself):

1. sql/warehouse/schema.sql: the dw schema and its three tables;
2. CREATE ROLE loader, unless it exists (Redshift has no IF NOT EXISTS for roles);
3. sql/warehouse/grants.sql: what loader may do.

Usage:
    python scripts/setup_warehouse.py --workgroup rrl-warehouse --database reviews --secret-arn <arn>
"""
import argparse
import os
import sys

import boto3

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, os.path.join(ROOT, "src"))

from lakehouse.warehouse import RedshiftData  # noqa: E402

ROLES = ["loader"]


def statements(path):
    """The SQL statements in a file, comments removed, split on semicolons."""
    with open(path, encoding="utf-8") as handle:
        lines = [line.split("--", 1)[0] for line in handle]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def setup(redshift, roles=ROLES):
    existing = {r["role_name"] for r in redshift.query("SELECT role_name FROM svv_roles")}
    redshift.run(statements(os.path.join(ROOT, "sql", "warehouse", "schema.sql")))
    for role in roles:
        if role not in existing:
            redshift.run([f"CREATE ROLE {role}"])
    redshift.run(statements(os.path.join(ROOT, "sql", "warehouse", "grants.sql")))
    return sorted(set(roles) - existing)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--workgroup", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--secret-arn", required=True, help="the admin password secret (terraform output)")
    parser.add_argument("--region", default="ap-southeast-2")
    args = parser.parse_args()

    client = boto3.client("redshift-data", region_name=args.region)
    redshift = RedshiftData(client, args.workgroup, args.database, secret_arn=args.secret_arn)
    created = setup(redshift)
    print(f"schema dw ready; roles created: {', '.join(created) or 'none (already there)'}")
    for row in redshift.query("SELECT table_name FROM svv_tables WHERE table_schema = 'dw' ORDER BY 1"):
        print(f"  dw.{row['table_name']}")


if __name__ == "__main__":
    main()
