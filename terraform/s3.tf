# terraform/s3.tf

resource "aws_s3_bucket" "latest_dam_data_storage" {
  bucket = "latest-dam-data-storage-${var.AWS_ACCOUNT_ID}"  # Ensure bucket name uniqueness

  tags = {
    Environment = "production"
    Name        = "LatestDamDataStorage"
  }
}

resource "aws_s3_bucket_notification" "latest_dam_data_storage_notification" {
  bucket = aws_s3_bucket.latest_dam_data_storage.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_load_rds_glue.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = ""   # Optionally specify a prefix
    filter_suffix       = ""   # Optionally specify a suffix
  }

  depends_on = [aws_lambda_permission.allow_s3_invoke_lambda_load_rds_glue]
}
