resource "aws_cloudwatch_log_metric_filter" "ingestion_error_alert" {
  name           = "alert_error_metric_filter"
  pattern        = "ERROR"
  log_group_name = "/aws/lambda/ingestion-lambda"

  metric_transformation {
    name      = "IngestionErrorCount"
    namespace = "Ingestion"
    value     = "1"
  }
}


resource "aws_cloudwatch_log_metric_filter" "remodelling_error_alert" {
  name           = "alert_error_metric_filter"
  pattern        = "ERROR"
  log_group_name = "/aws/lambda/remodelling-lambda"

  metric_transformation {
    name      = "RemodellingErrorCount"
    namespace = "Remodelling"
    value     = "1"
  }
}


resource "aws_cloudwatch_log_metric_filter" "loading_error_alert" {
  name           = "alert_error_metric_filter"
  pattern        = "ERROR"
  log_group_name = "aws/lambda/${aws_lambda_function.loading_lambda.function_name}"


  metric_transformation {
    name      = "LoadingErrorCount"
    namespace = "Loading"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "ingestion_error_alarm" {
  alarm_name          = "Ingestion-error-alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "ErrorCount"
  namespace           = "Ingestion"
  period              = 300
  statistic           = "Average"
  threshold           = 1
  alarm_description   = "Ingestion lambda encountered an error"
  actions_enabled     = "true"
  alarm_actions       = [aws_sns_topic.error_notification.arn]
}

resource "aws_cloudwatch_metric_alarm" "remodelling_error_alarm" {
  alarm_name          = "Remodelling-error-alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "ErrorCount"
  namespace           = "Remodelling"
  period              = 300
  statistic           = "Average"
  threshold           = 1
  alarm_description   = "Remodelling lambda encountered an error"
  actions_enabled     = "true"
  alarm_actions       = [aws_sns_topic.error_notification.arn]
}

resource "aws_cloudwatch_metric_alarm" "loading_error_alarm" {
  alarm_name          = "Loading-error-alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "ErrorCount"
  namespace           = "Loading"
  period              = 300
  statistic           = "Average"
  threshold           = 1
  alarm_description   = "Loading lambda encountered an error"
  actions_enabled     = "true"
  alarm_actions       = [aws_sns_topic.error_notification.arn]
}
