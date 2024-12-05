# terraform/sns.tf

resource "aws_sns_topic" "eventbridge_notifications" {
  name = "eventbridge-notifications"
}

resource "aws_sns_topic_subscription" "email_subscription" {
  topic_arn = aws_sns_topic.eventbridge_notifications.arn
  protocol  = "email"
  endpoint  = var.NOTIFICATION_EMAIL
}
