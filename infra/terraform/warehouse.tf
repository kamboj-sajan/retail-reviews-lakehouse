# The warehouse (phase 4): Redshift Serverless, loaded by the load_warehouse
# Glue job after each month passes the quality gate.
#
#   load_warehouse (Glue)  writes load files to s3://<artifacts>/warehouse/
#                          then, through the Redshift Data API, runs one
#                          transaction: COPY into staging, delete, insert
#   Redshift               reads those files with the copy role below
#
# Cost (Sydney, AWS Price List API): $0.419 per RPU-hour of compute, billed
# per second while queries run (60-second minimum), nothing while idle; plus
# $0.0261 per GB-month of managed storage. The usage limit below turns user
# queries off for the rest of the month once its RPU-hours are used up.

# --- network -----------------------------------------------------------------

# A workgroup needs subnets, and the account's default VPC has one in each of
# Sydney's three availability zones. Nothing connects to the workgroup over
# the network: the Data API is an HTTPS service, and the workgroup is not
# publicly accessible.
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

# --- namespace (storage, users) and workgroup (compute) ------------------------

resource "aws_redshiftserverless_namespace" "warehouse" {
  namespace_name = "${var.bucket_prefix}-warehouse"
  db_name        = var.redshift_database

  # Redshift generates the admin password and keeps it in Secrets Manager
  # ($0.40 a month), so it never appears in Terraform state or in this repo.
  # The admin user runs sql/warehouse/schema.sql once; loads do not use it.
  admin_username        = "rrl_admin"
  manage_admin_password = true

  # The role COPY uses to read the load files.
  iam_roles            = [aws_iam_role.redshift_copy.arn]
  default_iam_role_arn = aws_iam_role.redshift_copy.arn
}

resource "aws_redshiftserverless_workgroup" "warehouse" {
  namespace_name = aws_redshiftserverless_namespace.warehouse.namespace_name
  workgroup_name = "${var.bucket_prefix}-warehouse"

  # 4 RPUs is the smallest base capacity, and enough for 64 million rows.
  # max_capacity stops it scaling up past this many RPUs.
  base_capacity = var.redshift_base_rpu
  max_capacity  = var.redshift_max_rpu

  publicly_accessible  = false
  enhanced_vpc_routing = false
  subnet_ids           = data.aws_subnets.default.ids
}

# The hard limit on spend that the budget alarm cannot be (decisions.md,
# "Budget alarm"): once the month's RPU-hours are used, queries are refused,
# loads fail and the alert email goes out.
resource "aws_redshiftserverless_usage_limit" "compute" {
  resource_arn  = aws_redshiftserverless_workgroup.warehouse.arn
  usage_type    = "serverless-compute"
  amount        = var.redshift_max_rpu_hours_per_month
  period        = "monthly"
  breach_action = "deactivate"
}

# --- the role Redshift uses to read load files --------------------------------

data "aws_iam_policy_document" "redshift_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com", "redshift-serverless.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_iam_role" "redshift_copy" {
  name               = "${var.bucket_prefix}-redshift-copy"
  description        = "Lets ${var.project}'s Redshift read its load files."
  assume_role_policy = data.aws_iam_policy_document.redshift_trust.json
}

# Read the load files and nothing else: not the lake, not the rest of artifacts.
data "aws_iam_policy_document" "redshift_copy" {
  statement {
    sid       = "ListLoadFiles"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [local.artifacts.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${local.warehouse_prefix}/*"]
    }
  }
  statement {
    sid       = "ReadLoadFiles"
    actions   = ["s3:GetObject"]
    resources = ["${local.artifacts.arn}/${local.warehouse_prefix}/*"]
  }
}

resource "aws_iam_role_policy" "redshift_copy" {
  name   = "read-load-files"
  role   = aws_iam_role.redshift_copy.id
  policy = data.aws_iam_policy_document.redshift_copy.json
}

# --- the role the load job runs as --------------------------------------------

locals {
  warehouse_prefix = "warehouse"
  # Jobs that run as the warehouse role instead of the lake role.
  warehouse_jobs = ["load_warehouse"]
}

# A role of its own, so that the lake jobs cannot touch the warehouse and the
# load job cannot write to the lake.
resource "aws_iam_role" "glue_warehouse" {
  name               = "${var.bucket_prefix}-glue-warehouse"
  description        = "Runs the ${var.project} warehouse load job."
  assume_role_policy = data.aws_iam_policy_document.glue_trust.json

  # When this role signs in to Redshift through the Data API, Redshift
  # creates the database user IAMR:<role name> and gives it the database
  # roles named here. sql/warehouse/grants.sql decides what "loader" may do.
  tags = {
    RedshiftDbRoles = "loader"
  }
}

data "aws_iam_policy_document" "glue_warehouse" {
  statement {
    sid       = "ListBuckets"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lake["cleaned"].arn, aws_s3_bucket.lake["curated"].arn, local.artifacts.arn]
  }
  statement {
    sid       = "ReadCleanedCuratedAndCode"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lake["cleaned"].arn}/*", "${aws_s3_bucket.lake["curated"].arn}/*", "${local.artifacts.arn}/glue/*"]
  }
  statement {
    sid = "WriteLoadFilesLogsTempAndMetrics"
    actions = [
      "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
      "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts",
    ]
    resources = [
      "${local.artifacts.arn}/${local.warehouse_prefix}/*",
      "${local.artifacts.arn}/spark-logs/*",
      "${local.artifacts.arn}/glue-temp/*",
      "${local.artifacts.arn}/metrics/*",
    ]
  }
  statement {
    sid       = "WriteGlueLogs"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:${var.aws_region}:${local.account_id}:log-group:/aws-glue/*"]
  }

  # Run SQL in this workgroup only, signed in as this role.
  statement {
    sid       = "RunSqlInTheWarehouse"
    actions   = ["redshift-data:ExecuteStatement", "redshift-data:BatchExecuteStatement"]
    resources = [aws_redshiftserverless_workgroup.warehouse.arn]
  }
  statement {
    sid       = "SignInToTheWarehouse"
    actions   = ["redshift-serverless:GetCredentials"]
    resources = [aws_redshiftserverless_workgroup.warehouse.arn]
  }
  # Statement IDs are not ARNs, so these cannot name a resource. The
  # condition limits them to statements this role started itself.
  statement {
    sid       = "FollowOwnStatements"
    actions   = ["redshift-data:DescribeStatement", "redshift-data:GetStatementResult", "redshift-data:CancelStatement"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "redshift-data:statement-owner-iam-userid"
      values   = ["$${aws:userid}"]
    }
  }
}

resource "aws_iam_role_policy" "glue_warehouse" {
  name   = "warehouse-load"
  role   = aws_iam_role.glue_warehouse.id
  policy = data.aws_iam_policy_document.glue_warehouse.json
}

# --- outputs -------------------------------------------------------------------

output "redshift_workgroup" {
  value = aws_redshiftserverless_workgroup.warehouse.workgroup_name
}

output "redshift_database" {
  value = var.redshift_database
}

output "redshift_admin_secret_arn" {
  description = "Secrets Manager secret holding the admin password; used only by scripts/setup_warehouse.py."
  value       = aws_redshiftserverless_namespace.warehouse.admin_password_secret_arn
}
