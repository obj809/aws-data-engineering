# terraform/secrets.tf

# provider "aws" {
#   region  = var.CUSTOM_AWS_REGION
#   profile = "default"  # Use an appropriate profile if needed
# }

# resource "aws_secretsmanager_secret" "api_secrets" {
#   name = "api_secrets"

#   tags = {
#     "Environment" = "production"
#   }
# }

# resource "aws_secretsmanager_secret_version" "api_secrets_version" {
#   secret_id     = aws_secretsmanager_secret.api_secrets.id
#   secret_string = jsonencode({
#     API_KEY    = var.API_KEY
#     API_SECRET = var.API_SECRET
#   })
# }
