# terraform/glue.tf

resource "aws_s3_bucket" "glue_scripts" {
  bucket = "glue-scripts-${var.AWS_ACCOUNT_ID}"

  tags = {
    Name        = "GlueScriptsBucket"
    Environment = "production"
  }
}

resource "aws_s3_bucket" "glue_temp" {
  bucket = "glue-temp-${var.AWS_ACCOUNT_ID}"

  tags = {
    Name        = "GlueTempBucket"
    Environment = "production"
  }
}

# Upload the Glue script to S3 using aws_s3_object
resource "aws_s3_object" "latest_dam_data_etl_script" {
  bucket = aws_s3_bucket.glue_scripts.bucket
  key    = "scripts/latest_dam_data_etl.py"
  source = "${path.module}/../glue_scripts/latest_dam_data_etl.py"
}

# Define the Glue job
resource "aws_glue_job" "latest_dam_data_etl" {
  name     = "latest_dam_data_etl"
  role_arn = aws_iam_role.glue_service_role.arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/scripts/latest_dam_data_etl.py"
  }

  default_arguments = {
    "--job-language" = "python"
    "--TempDir"      = "s3://${aws_s3_bucket.glue_temp.bucket}/temp/"
  }

  max_retries       = 0
  timeout           = 10
  glue_version      = "2.0"
  number_of_workers = 2
  worker_type       = "Standard"
}
