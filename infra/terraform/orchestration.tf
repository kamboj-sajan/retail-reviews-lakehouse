# Monthly incremental load: a month "arrives" -> EventBridge -> Step Functions
# -> Glue (clean_reviews, then build_curated) -> email on failure.
#
#   scripts/replay_month.py 2023-01
#     copies   raw/feed/reviews/category=*/review_month=2023-01/*
#     to       raw/landing/reviews/category=*/review_month=2023-01/*
#     then writes the marker raw/landing/_arrived/2023-01
#   S3 sends "Object Created" for the marker to EventBridge
#   the rule below matches only keys under landing/_arrived/ and starts the
#   state machine with {"key": "landing/_arrived/2023-01"}
#
# The marker is written last, after every file of the month, so the pipeline
# never starts on a half-delivered month. One marker per month also means one
# execution per month, instead of one per data file.

locals {
  raw_bucket     = aws_s3_bucket.lake["raw"].id
  arrival_prefix = "landing/_arrived/"
  monthly_jobs   = ["clean_reviews", "build_curated", "check_quality"]
  state_machine  = "${var.bucket_prefix}-monthly-load"
  glue_job_arns  = [for name in local.monthly_jobs : "arn:aws:glue:${var.aws_region}:${local.account_id}:job/${aws_glue_job.job[name].name}"]
  glue_task_retry = [
    # Another month's run of the same job is still going (each Glue job allows
    # one run at a time). Wait and try again; this is expected, not a failure.
    {
      ErrorEquals     = ["Glue.ConcurrentRunsExceededException"]
      IntervalSeconds = 60
      MaxAttempts     = 20
      BackoffRate     = 1.2
    },
    # The Glue run itself failed, for example because a FLEX worker was taken
    # back. Retry once after a pause; a second failure goes to the alert.
    {
      ErrorEquals     = ["States.TaskFailed"]
      IntervalSeconds = 120
      MaxAttempts     = 1
      BackoffRate     = 2
    },
  ]
}

# --- failure notification ----------------------------------------------------

resource "aws_sns_topic" "alerts" {
  name = "${var.bucket_prefix}-pipeline-alerts"
}

# AWS emails a confirmation link to this address; no alert is delivered until
# it is clicked.
resource "aws_sns_topic_subscription" "alerts_email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# --- the state machine ---------------------------------------------------------

resource "aws_sfn_state_machine" "monthly_load" {
  name     = local.state_machine
  role_arn = aws_iam_role.monthly_load.arn
  type     = "STANDARD"

  definition = jsonencode({
    Comment        = "Load one month: clean the reviews, then rebuild that month's curated tables."
    TimeoutSeconds = 7200
    StartAt        = "HasMonth"
    States = {
      # Input is either {"month": "2023-01"} (run by hand or by the
      # idempotency test) or {"key": "landing/_arrived/2023-01"} (EventBridge).
      HasMonth = {
        Type    = "Choice"
        Choices = [{ Variable = "$.month", IsPresent = true, Next = "CheckMonth" }]
        Default = "MonthFromArrivalKey"
      }
      MonthFromArrivalKey = {
        Type       = "Pass"
        Parameters = { "month.$" = "States.ArrayGetItem(States.StringSplit($.key, '/'), 2)" }
        Next       = "CheckMonth"
      }
      # A first, loose check. StringMatches only understands "*" as a
      # wildcard (not "?"), so it cannot express YYYY-MM exactly; the Glue
      # jobs check the exact format (--month) and fail on anything else.
      CheckMonth = {
        Type = "Choice"
        Choices = [{
          And = [
            { Variable = "$.month", IsString = true },
            { Variable = "$.month", StringMatches = "*-*" },
          ]
          Next = "CleanReviews"
        }]
        Default = "BadMonth"
      }
      BadMonth = {
        Type       = "Pass"
        Parameters = { "Error" = "BadInput", "Cause" = "month must look like YYYY-MM" }
        ResultPath = "$.error"
        Next       = "NotifyFailure"
      }

      # .sync: Step Functions starts the Glue run and waits for it to finish,
      # so a failed Glue run becomes a failed state here.
      CleanReviews = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.job["clean_reviews"].name
          Arguments = {
            "--month.$"  = "$.month"
            "--raw_path" = "s3://${local.raw_bucket}/landing"
          }
        }
        ResultSelector = { "run_id.$" = "$.Id", "state.$" = "$.JobRunState" }
        ResultPath     = "$.clean_reviews"
        Retry          = local.glue_task_retry
        Catch          = [{ ErrorEquals = ["States.ALL"], ResultPath = "$.error", Next = "NotifyFailure" }]
        Next           = "BuildCurated"
      }
      BuildCurated = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName   = aws_glue_job.job["build_curated"].name
          Arguments = { "--month.$" = "$.month" }
        }
        ResultSelector = { "run_id.$" = "$.Id", "state.$" = "$.JobRunState" }
        ResultPath     = "$.build_curated"
        Retry          = local.glue_task_retry
        Catch          = [{ ErrorEquals = ["States.ALL"], ResultPath = "$.error", Next = "NotifyFailure" }]
        Next           = "CheckQuality"
      }
      # The quality gate. If any check fails, the Glue run fails, this state
      # fails, the alert goes out and nothing after it runs. Only "job busy" is
      # retried here: a failed check gives the same answer the second time,
      # so retrying it would only delay the alert.
      CheckQuality = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.job["check_quality"].name
          Arguments = {
            "--month.$"  = "$.month"
            "--raw_path" = "s3://${local.raw_bucket}/landing"
          }
        }
        ResultSelector = { "run_id.$" = "$.Id", "state.$" = "$.JobRunState" }
        ResultPath     = "$.check_quality"
        Retry          = [local.glue_task_retry[0]]
        Catch          = [{ ErrorEquals = ["States.ALL"], ResultPath = "$.error", Next = "NotifyFailure" }]
        Next           = "Loaded"
      }
      Loaded = { Type = "Succeed" }

      NotifyFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn    = aws_sns_topic.alerts.arn
          Subject     = "${var.project}: monthly load failed"
          "Message.$" = "States.Format('The monthly load for {} failed.\n\nError: {}\nCause: {}\n\nExecution: {}', $.month, $.error.Error, $.error.Cause, $$.Execution.Id)"
        }
        ResultPath = null
        Next       = "LoadFailed"
      }
      LoadFailed = { Type = "Fail", Error = "MonthlyLoadFailed", Cause = "See the notification email or the execution history." }
    }
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.monthly_load.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }
}

resource "aws_cloudwatch_log_group" "monthly_load" {
  # Step Functions requires log groups under /aws/vendedlogs/ for large policies.
  name              = "/aws/vendedlogs/states/${local.state_machine}"
  retention_in_days = 14
}

data "aws_iam_policy_document" "states_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_iam_role" "monthly_load" {
  name               = "${var.bucket_prefix}-monthly-load"
  description        = "Runs the ${var.project} monthly load state machine."
  assume_role_policy = data.aws_iam_policy_document.states_trust.json
}

data "aws_iam_policy_document" "monthly_load" {
  # Start the two monthly Glue jobs and watch them; nothing else in Glue.
  statement {
    sid       = "RunMonthlyGlueJobs"
    actions   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
    resources = local.glue_job_arns
  }
  statement {
    sid       = "SendAlerts"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
  }
  # Step Functions log delivery uses these account-level calls; AWS does not
  # support narrowing them to one log group.
  statement {
    sid = "DeliverExecutionLogs"
    actions = [
      "logs:CreateLogDelivery", "logs:GetLogDelivery", "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery", "logs:ListLogDeliveries", "logs:PutResourcePolicy",
      "logs:DescribeResourcePolicies", "logs:DescribeLogGroups",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "monthly_load" {
  name   = "run-monthly-load"
  role   = aws_iam_role.monthly_load.id
  policy = data.aws_iam_policy_document.monthly_load.json
}

# --- EventBridge trigger -------------------------------------------------------

# Send this bucket's events to EventBridge. (This replaces any other
# notification configuration on the bucket; there is none.)
resource "aws_s3_bucket_notification" "raw_to_eventbridge" {
  bucket      = local.raw_bucket
  eventbridge = true
}

resource "aws_cloudwatch_event_rule" "month_arrived" {
  name        = "${var.bucket_prefix}-month-arrived"
  description = "A month's data has landed: start the monthly load."
  event_pattern = jsonencode({
    source        = ["aws.s3"]
    "detail-type" = ["Object Created"]
    detail = {
      bucket = { name = [local.raw_bucket] }
      object = { key = [{ prefix = local.arrival_prefix }] }
    }
  })
}

resource "aws_cloudwatch_event_target" "start_monthly_load" {
  rule     = aws_cloudwatch_event_rule.month_arrived.name
  arn      = aws_sfn_state_machine.monthly_load.arn
  role_arn = aws_iam_role.events_start_load.arn

  input_transformer {
    input_paths    = { key = "$.detail.object.key" }
    input_template = "{\"key\": <key>}"
  }

  retry_policy {
    maximum_event_age_in_seconds = 3600
    maximum_retry_attempts       = 10
  }
}

data "aws_iam_policy_document" "events_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_iam_role" "events_start_load" {
  name               = "${var.bucket_prefix}-events-start-load"
  description        = "Lets EventBridge start the ${var.project} monthly load."
  assume_role_policy = data.aws_iam_policy_document.events_trust.json
}

resource "aws_iam_role_policy" "events_start_load" {
  name = "start-monthly-load"
  role = aws_iam_role.events_start_load.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "states:StartExecution"
      Resource = aws_sfn_state_machine.monthly_load.arn
    }]
  })
}

output "monthly_load_state_machine" {
  value = aws_sfn_state_machine.monthly_load.arn
}
