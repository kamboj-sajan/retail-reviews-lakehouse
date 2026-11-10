variable "aws_region" {
  description = "Region for every resource. Mumbai is closest to me."
  type        = string
  default     = "ap-south-1"
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

variable "alert_email" {
  description = "Where budget alerts are sent. Set it in terraform.tfvars (git-ignored), never in a committed file."
  type        = string

  validation {
    condition     = can(regex("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", var.alert_email))
    error_message = "alert_email must be an email address."
  }
}
