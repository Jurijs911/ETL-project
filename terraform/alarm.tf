resource "aws_cloudwatch_log_metric_filter" "alert_error" {
  name           = "alert_error_metric_filter"
  pattern        = "ERROR"
  log_group_name = "" # also needs to be added

  metric_transformation {
    name      = "ErrorCount"
    namespace = "AWS/EC2"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "alert_errors" {
  alarm_name          = "error_alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "ErrorCount"
  namespace           = "AWS/EC2"
  period              = 300
  statistic           = "Average"
  threshold           = 1
  alarm_description   = "This alarm is triggered when ERROR appears"
  actions_enabled     = "true"
  alarm_actions       = [aws_sns_topic.error_notification.arn] # add alarm arn from terminal/console

}
