# Glue Data Catalog: one database per layer, one table per dataset.
#
# Why tables are defined here and not found by a Glue crawler
# ------------------------------------------------------------
# A crawler samples the files and guesses the schema. It would have to guess
# that review_month is text rather than a date, it costs DPU time on every
# run, and its result is not reviewed anywhere. These definitions are the
# schema the jobs write (docs/data-lake-layout.md), checked into git.
#
# Why partition projection
# ------------------------
# Normally every partition folder must be registered in the catalog before
# Athena sees it (a crawler, MSCK REPAIR TABLE, or ALTER TABLE ADD PARTITION
# after each load). With projection, Athena computes the possible partitions
# from rules in the table properties: category is one of a fixed list, and
# review_month is any month from the start of the range up to now. A new
# month written by the pipeline is queryable immediately with no catalog
# step. Projection is an Athena feature: Spark and Redshift Spectrum ignore
# it, which is fine here because the jobs read S3 paths directly and Redshift
# loads with COPY.

locals {
  categories = jsondecode(file("${local.repo_root}/config/categories.json")).categories

  parquet_format = {
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    serde         = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
  }

  category_projection = {
    "projection.category.type"   = "enum"
    "projection.category.values" = join(",", local.categories)
  }

  # Months before the first review in the data simply return no rows.
  month_projection = {
    "projection.review_month.type"          = "date"
    "projection.review_month.format"        = "yyyy-MM"
    "projection.review_month.range"         = "1995-01,NOW"
    "projection.review_month.interval"      = "1"
    "projection.review_month.interval.unit" = "MONTHS"
  }

  measure_columns = [
    { name = "review_count", type = "bigint" },
    { name = "rating_sum", type = "bigint" },
    { name = "avg_rating", type = "double" },
    { name = "verified_count", type = "bigint" },
  ]

  bucket = { for layer, b in aws_s3_bucket.lake : layer => b.id }

  # Every Parquet table, keyed "<database>.<table>".
  parquet_tables = {
    "cleaned.reviews" = {
      location   = "s3://${local.bucket.cleaned}/reviews"
      partitions = ["category", "review_month"]
      columns = [
        { name = "review_id", type = "string" },
        { name = "user_id", type = "string" },
        { name = "asin", type = "string" },
        { name = "parent_asin", type = "string" },
        { name = "rating", type = "int" },
        { name = "helpful_vote", type = "int" },
        { name = "verified_purchase", type = "boolean" },
        { name = "title", type = "string" },
        { name = "text", type = "string" },
        { name = "image_count", type = "int" },
        { name = "review_ts", type = "timestamp" },
        { name = "review_date", type = "date" },
      ]
    }
    "cleaned.products" = {
      location   = "s3://${local.bucket.cleaned}/products"
      partitions = ["category"]
      columns = [
        { name = "parent_asin", type = "string" },
        { name = "title", type = "string" },
        { name = "main_category", type = "string" },
        { name = "store", type = "string" },
        { name = "price", type = "double" },
        { name = "average_rating", type = "double" },
        { name = "rating_number", type = "int" },
        { name = "categories", type = "array<string>" },
        { name = "details_json", type = "string" },
      ]
    }
    "curated.reviews_enriched" = {
      location   = "s3://${local.bucket.curated}/reviews_enriched"
      partitions = ["category", "review_month"]
      columns = [
        { name = "review_id", type = "string" },
        { name = "user_id", type = "string" },
        { name = "asin", type = "string" },
        { name = "parent_asin", type = "string" },
        { name = "rating", type = "int" },
        { name = "helpful_vote", type = "int" },
        { name = "verified_purchase", type = "boolean" },
        { name = "image_count", type = "int" },
        { name = "review_ts", type = "timestamp" },
        { name = "review_date", type = "date" },
        { name = "has_product", type = "boolean" },
        { name = "product_title", type = "string" },
        { name = "store", type = "string" },
        { name = "price", type = "double" },
        { name = "main_category", type = "string" },
      ]
    }
    "curated.category_monthly" = {
      location   = "s3://${local.bucket.curated}/category_monthly"
      partitions = ["review_month"]
      columns    = concat([{ name = "category", type = "string" }], local.measure_columns)
    }
    "curated.category_daily" = {
      location   = "s3://${local.bucket.curated}/category_daily"
      partitions = ["review_month"]
      columns = concat([
        { name = "category", type = "string" },
        { name = "review_date", type = "date" },
      ], local.measure_columns)
    }
    "curated.product_monthly" = {
      location   = "s3://${local.bucket.curated}/product_monthly"
      partitions = ["review_month"]
      columns = concat([
        { name = "category", type = "string" },
        { name = "parent_asin", type = "string" },
        { name = "product_title", type = "string" },
        { name = "store", type = "string" },
      ], local.measure_columns)
    }
  }
}

resource "aws_glue_catalog_database" "layer" {
  for_each = toset(["raw", "cleaned", "curated"])

  # Glue and Athena database names may not contain hyphens.
  name        = "${var.bucket_prefix}_${each.key}"
  description = "${var.project}: ${each.key} layer"
}

# --- Parquet tables (cleaned and curated) -----------------------------------

resource "aws_glue_catalog_table" "parquet" {
  for_each = local.parquet_tables

  database_name = aws_glue_catalog_database.layer[split(".", each.key)[0]].name
  name          = split(".", each.key)[1]
  table_type    = "EXTERNAL_TABLE"

  parameters = merge(
    {
      "classification"            = "parquet"
      "projection.enabled"        = "true"
      "storage.location.template" = join("/", concat([each.value.location], [for p in each.value.partitions : "${p}=$${${p}}"]))
    },
    contains(each.value.partitions, "category") ? local.category_projection : {},
    contains(each.value.partitions, "review_month") ? local.month_projection : {},
  )

  dynamic "partition_keys" {
    for_each = each.value.partitions
    content {
      name = partition_keys.value
      type = "string"
    }
  }

  storage_descriptor {
    location      = each.value.location
    input_format  = local.parquet_format.input_format
    output_format = local.parquet_format.output_format

    ser_de_info {
      serialization_library = local.parquet_format.serde
    }

    dynamic "columns" {
      for_each = each.value.columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

# --- raw JSON tables ----------------------------------------------------------
# These exist so the same question can be asked of raw JSON and of Parquet in
# Athena (a phase 7 benchmark), and to inspect raw data without downloading it.

locals {
  raw_tables = {
    reviews = {
      prefix = "reviews"
      columns = [
        { name = "user_id", type = "string" },
        { name = "asin", type = "string" },
        { name = "parent_asin", type = "string" },
        { name = "rating", type = "double" },
        { name = "title", type = "string" },
        { name = "text", type = "string" },
        # A reserved word in Athena SQL: write it as "timestamp" in queries.
        { name = "timestamp", type = "bigint" },
        { name = "helpful_vote", type = "bigint" },
        { name = "verified_purchase", type = "boolean" },
      ]
    }
    products = {
      prefix = "meta"
      columns = [
        { name = "parent_asin", type = "string" },
        { name = "title", type = "string" },
        { name = "main_category", type = "string" },
        { name = "store", type = "string" },
        { name = "price", type = "double" },
        { name = "average_rating", type = "double" },
        { name = "rating_number", type = "bigint" },
        { name = "categories", type = "array<string>" },
      ]
    }
  }
}

resource "aws_glue_catalog_table" "raw" {
  for_each = local.raw_tables

  database_name = aws_glue_catalog_database.layer["raw"].name
  name          = each.key
  table_type    = "EXTERNAL_TABLE"

  parameters = merge(
    {
      "classification"            = "json"
      "compressionType"           = "gzip"
      "projection.enabled"        = "true"
      "storage.location.template" = "s3://${local.bucket.raw}/${each.value.prefix}/category=$${category}"
    },
    local.category_projection,
  )

  partition_keys {
    name = "category"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${local.bucket.raw}/${each.value.prefix}"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
      parameters = {
        # A malformed line becomes a row of NULLs instead of failing the query.
        "ignore.malformed.json" = "true"
      }
    }

    # Fields not listed here (images, details, ...) are ignored by the SerDe.
    dynamic "columns" {
      for_each = each.value.columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}
