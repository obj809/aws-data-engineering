# terraform/eventbridge.tf

# Rule to trigger Lambda data collection and send SNS notification when a secret is updated
resource "aws_cloudwatch_event_rule" "trigger_data_collection" {
  name           = "trigger_data_collection"
  description    = "Rule to trigger lambda_data_collection and send SNS notification when secret is updated"
  event_bus_name = "default"
  event_pattern  = jsonencode({
    "source": ["lambda_trigger"],
    "detail-type": ["SecretUpdated"]
  })
}

resource "aws_lambda_permission" "allow_eventbridge_to_invoke_data_collection" {
  statement_id  = "AllowEventBridgeInvokeDataCollection"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_data_collection.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.trigger_data_collection.arn
}

resource "aws_cloudwatch_event_target" "data_collection_target" {
  rule = aws_cloudwatch_event_rule.trigger_data_collection.name
  arn  = aws_lambda_function.lambda_data_collection.arn
}

resource "aws_cloudwatch_event_target" "sns_notification_target" {
  rule = aws_cloudwatch_event_rule.trigger_data_collection.name
  arn  = aws_sns_topic.eventbridge_notifications.arn
}

# Rule to detect Glue job completion and trigger SNS notification
resource "aws_cloudwatch_event_rule" "glue_job_completion_rule" {
  name        = "glue_job_completion_rule"
  description = "Rule triggered when Glue job successfully completes"
  event_pattern = jsonencode({
    "source": ["aws.glue"],
    "detail-type": ["Glue Job State Change"],
    "detail": {
      "state": ["SUCCEEDED"],
      "jobName": ["${aws_glue_job.latest_dam_data_etl.name}"] # Replace with actual Glue job name if needed
    }
  })
}

resource "aws_cloudwatch_event_target" "glue_completion_to_sns" {
  rule = aws_cloudwatch_event_rule.glue_job_completion_rule.name
  arn  = aws_sns_topic.eventbridge_notifications.arn
}

# Allow EventBridge to publish events to the SNS topic
resource "aws_sns_topic_policy" "allow_eventbridge_to_publish" {
  arn = aws_sns_topic.eventbridge_notifications.arn
  policy = jsonencode({
    Version   = "2012-10-17",
    Statement = [
      {
        Effect    = "Allow",
        Principal = {
          Service = "events.amazonaws.com"
        },
        Action    = "sns:Publish",
        Resource  = aws_sns_topic.eventbridge_notifications.arn
      }
    ]
  })
}
