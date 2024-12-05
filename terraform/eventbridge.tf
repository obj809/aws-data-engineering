# terraform/eventbridge.tf

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
