# Budget alarm: emails me as spending approaches the limit.
#
# Two things to know about AWS Budgets:
# 1. It is an alarm, not a brake. It does not stop any resource. Billing data
#    also arrives hours late, so an alert can lag the actual spend.
# 2. Budget periods are calendar based (monthly here). There is no "lifetime
#    total" period, so the limit applies to each month separately.
resource "aws_budgets_budget" "project" {
  name         = "${var.project}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  # By default a budget measures cost AFTER credits are subtracted. This
  # account pays with free-plan credits, so that number would stay at zero and
  # the alarm would never fire. Excluding credits and refunds makes the budget
  # track what the usage actually costs.
  cost_types {
    include_credit = false
    include_refund = false
  }

  # Alerts on real spend at 50%, 80% and 100% of the limit.
  dynamic "notification" {
    for_each = [50, 80, 100]
    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value
      threshold_type             = "PERCENTAGE"
      notification_type          = "ACTUAL"
      subscriber_email_addresses = [var.alert_email]
    }
  }

  # Early warning: AWS forecasts the month will end over the limit.
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
}
