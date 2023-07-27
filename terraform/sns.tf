resource "aws_sns_topic" "error_notification" {
  name = "kp-error-topic"
}

resource "aws_sns_topic_subscription" "error_notification_endpoint" {
  topic_arn = aws_sns_topic.error_notification.arn
  protocol  = "email"
  endpoint  = "keypears@engineer.com"
}
