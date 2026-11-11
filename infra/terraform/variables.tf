variable "aws_region" {
  description = "Region for every resource. The AWS account this project runs in is restricted to Sydney."
  type        = string
  default     = "ap-southeast-2"
}

variable "aws_account_id" {
  description = "The only AWS account Terraform may touch. Set it in terraform.tfvars."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.aws_account_id))
    error_message = "aws_account_id must be a 12-digit account ID."
  }
}

variable "project" {
  description = "Project name, used in tags and resource names."
  type        = string
  default     = "retail-reviews-lakehouse"
}

variable "bucket_prefix" {
  description = "Short prefix for S3 bucket names (bucket names are global, max 63 chars)."
  type        = string
  default     = "rrl"
}

variable "monthly_budget_usd" {
  description = "Spending limit per calendar month that the budget alarm watches."
  type        = number
  default     = 50
}

variable "redshift_database" {
  description = "Name of the warehouse database in Redshift Serverless."
  type        = string
  default     = "reviews"
}

variable "redshift_base_rpu" {
  description = "Redshift Serverless base capacity in RPUs: 4, or a multiple of 8 from 8 up."
  type        = number
  default     = 4
}

variable "redshift_max_rpu" {
  description = "Most RPUs Redshift Serverless may scale up to."
  type        = number
  default     = 8
}

variable "redshift_max_rpu_hours_per_month" {
  description = "RPU-hours per calendar month after which Redshift refuses queries. 24 at $0.419 = about $10."
  type        = number
  default     = 24
}

variable "alert_email" {
  description = "Where budget alerts are sent. Set it in terraform.tfvars (git-ignored), never in a committed file."
  type        = string

  validation {
    condition     = can(regex("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", var.alert_email))
    error_message = "alert_email must be an email address."
  }
}
