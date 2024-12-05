# terraform/iam.tf

resource "aws_iam_role" "lambda_execution_role" {
  name = "LambdaExecutionRole"

  assume_role_policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Action": "sts:AssumeRole",
        "Effect": "Allow",
        "Principal": {
          "Service": "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy" "lambda_execution_policy" {
  name        = "LambdaExecutionPolicy"
  description = "Policy to allow Lambda to write to S3, write to CloudWatch logs, access Secrets Manager, interact with EventBridge and SNS, and connect to RDS"

  policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        # Permissions for CloudWatch Logs
        "Effect": "Allow",
        "Action": [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        "Resource": "arn:aws:logs:*:*:*"
      },
      {
        # Permissions to get and put the secret value
        "Effect": "Allow",
        "Action": [
          "secretsmanager:GetSecretValue",
          "secretsmanager:PutSecretValue"
        ],
        "Resource": "arn:aws:secretsmanager:${var.CUSTOM_AWS_REGION}:${var.AWS_ACCOUNT_ID}:secret:${var.SECRET_NAME}*"
      },
      {
        # Allow Lambda to use the Internet (for API calls)
        "Effect": "Allow",
        "Action": [
          "ec2:CreateNetworkInterface",
          "ec2:DeleteNetworkInterface",
          "ec2:DescribeNetworkInterfaces",
          "ec2:AssignPrivateIpAddresses",
          "ec2:UnassignPrivateIpAddresses",
          "ec2:DescribeInstances",
          "ec2:DescribeSubnets",
          "ec2:DescribeSecurityGroups"
        ],
        "Resource": "*"
      },
      {
        # Permissions to put events to EventBridge
        "Effect": "Allow",
        "Action": [
          "events:PutEvents"
        ],
        "Resource": "*"
      },
      {
        # Permissions for EventBridge to publish to SNS
        "Effect": "Allow",
        "Action": [
          "sns:Publish"
        ],
        "Resource": "arn:aws:sns:${var.CUSTOM_AWS_REGION}:${var.AWS_ACCOUNT_ID}:eventbridge-notifications"
      },
      {
        # Permissions to write to the S3 bucket
        "Effect": "Allow",
        "Action": [
          "s3:PutObject"
        ],
        "Resource": "${aws_s3_bucket.latest_dam_data_storage.arn}/*"
      },
      {
        # Permissions to read from the S3 bucket
        "Effect": "Allow",
        "Action": [
          "s3:GetObject"
        ],
        "Resource": "${aws_s3_bucket.latest_dam_data_storage.arn}/*"
      },
      {
        # Permissions to list the S3 bucket (optional but recommended)
        "Effect": "Allow",
        "Action": [
          "s3:ListBucket"
        ],
        "Resource": "${aws_s3_bucket.latest_dam_data_storage.arn}"
      }
    ]
  })
}

resource "aws_iam_policy" "lambda_secrets_policy" {
  name        = "LambdaSecretsAccessPolicy"
  description = "Policy to allow Lambda functions to access Secrets Manager"

  policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "secretsmanager:GetSecretValue"
        ],
        "Resource": "arn:aws:secretsmanager:${var.CUSTOM_AWS_REGION}:${var.AWS_ACCOUNT_ID}:secret:${var.SECRET_NAME}*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_policy_attachment" {
  role       = aws_iam_role.lambda_execution_role.name
  policy_arn = aws_iam_policy.lambda_execution_policy.arn
}

resource "aws_iam_role_policy_attachment" "attach_lambda_secrets_policy" {
  role       = aws_iam_role.lambda_execution_role.name
  policy_arn = aws_iam_policy.lambda_secrets_policy.arn
}

# Glue service role
resource "aws_iam_role" "glue_service_role" {
  name = "GlueServiceRole"

  assume_role_policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": {
          "Service": "glue.amazonaws.com"
        },
        "Action": "sts:AssumeRole"
      }
    ]
  })
}

# Attach AWSGlueServiceRole policy to the Glue service role
resource "aws_iam_policy_attachment" "attach_AWSGlueServiceRole" {
  name       = "AttachAWSGlueServiceRole"
  roles      = [aws_iam_role.glue_service_role.name]
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Policy to allow Glue to access S3 buckets
resource "aws_iam_policy" "glue_s3_access_policy" {
  name        = "GlueS3AccessPolicy"
  description = "Policy to allow Glue job to access S3 buckets for scripts and temp data"

  policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        # Permissions to read from the Glue scripts bucket
        "Effect": "Allow",
        "Action": [
          "s3:GetObject",
          "s3:ListBucket"
        ],
        "Resource": [
          aws_s3_bucket.glue_scripts.arn,
          "${aws_s3_bucket.glue_scripts.arn}/*"
        ]
      },
      {
        # Permissions to read/write to the Glue temp bucket
        "Effect": "Allow",
        "Action": [
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:GetObject",
          "s3:ListBucket"
        ],
        "Resource": [
          aws_s3_bucket.glue_temp.arn,
          "${aws_s3_bucket.glue_temp.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_glue_s3_access_policy" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = aws_iam_policy.glue_s3_access_policy.arn
}

# Policy to allow Lambda to start Glue jobs
resource "aws_iam_policy" "lambda_glue_policy" {
  name        = "LambdaGlueStartJobPolicy"
  description = "Policy to allow Lambda functions to start Glue jobs"

  policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "glue:StartJobRun"
        ],
        "Resource": [
          aws_glue_job.latest_dam_data_etl.arn
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_lambda_glue_policy" {
  role       = aws_iam_role.lambda_execution_role.name
  policy_arn = aws_iam_policy.lambda_glue_policy.arn
}
