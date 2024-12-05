# terraform/glue.tf

# Create an S3 bucket to store Glue scripts
resource "aws_s3_bucket" "glue_scripts" {
  bucket = "glue-scripts-${var.AWS_ACCOUNT_ID}"

  tags = {
    Name        = "GlueScriptsBucket"
    Environment = "production"
  }
}

# Create an S3 bucket for Glue temporary data
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

  # Ensure the object is publicly readable if necessary
  # acl    = "private"
}

resource "aws_glue_job" "latest_dam_data_etl" {
  name     = "latest_dam_data_etl"
  role_arn = aws_iam_role.glue_service_role.arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/scripts/latest_dam_data_etl.py"
  }

  default_arguments = {
    "--job-language"              = "python"
    "--TempDir"                   = "s3://${aws_s3_bucket.glue_temp.bucket}/temp/"
    "--DB_HOST"                   = var.DB_HOST
    "--DB_PORT"                   = var.DB_PORT
    "--DB_NAME"                   = var.DB_NAME
    "--DB_USER"                   = var.DB_USER
    "--DB_PASSWORD"               = var.DB_PASSWORD
    "--additional-python-modules" = "pymysql"
    "--enable-continuous-log-filter" = "true"  # Set to "true" or "false" explicitly
    "--enable-metrics"            = "true"  # Set to "true" or "false" explicitly
  }

  glue_version      = "3.0"  # Updated from "2.0" to "3.0"
  max_retries       = 0
  timeout           = 10
  number_of_workers = 2
  worker_type       = "Standard"

  execution_property {
    max_concurrent_runs = 1
  }

  # Removed the connections parameter to avoid running in a VPC
}

# Removed the Glue connection resource since we're not using a VPC
# resource "aws_glue_connection" "glue_rds_connection" {
#   name = "glue-rds-connection"
#
#   connection_properties = {
#     JDBC_CONNECTION_URL = "jdbc:mysql://${var.DB_HOST}:${var.DB_PORT}/${var.DB_NAME}"
#     USERNAME            = var.DB_USER
#     PASSWORD            = var.DB_PASSWORD
#   }
#
#   physical_connection_requirements {
#     availability_zone       = data.aws_availability_zones.available.names[0]
#     security_group_id_list  = [aws_security_group.glue_security_group.id]
#     subnet_id               = aws_subnet.private_subnet.id
#   }
# }

# Removed the security group resources since they're not needed
# resource "aws_security_group" "glue_security_group" {
#   name        = "glue-security-group"
#   description = "Security group for Glue job to access RDS"
#   vpc_id      = aws_vpc.main.id
#
#   # Allow outbound access to RDS
#   egress {
#     from_port   = 0
#     to_port     = 0
#     protocol    = "-1"
#     cidr_blocks = ["0.0.0.0/0"]
#   }
# }
#
# resource "aws_security_group_rule" "rds_ingress_from_glue" {
#   type                     = "ingress"
#   from_port                = 3306
#   to_port                  = 3306
#   protocol                 = "tcp"
#   security_group_id        = aws_security_group.rds_security_group.id
#   source_security_group_id = aws_security_group.glue_security_group.id
# }
