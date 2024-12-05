# terraform/variables.tf

variable "AWS_REGION" {
  type        = string
  description = "The AWS region to deploy resources"
  default     = "ap-southeast-2"  # Update as needed
}

variable "AWS_ACCOUNT_ID" {
  type        = string
  description = "The AWS Account ID"
}

variable "DB_HOST" {
  type        = string
  description = "The hostname of the RDS instance."
}

variable "DB_PORT" {
  type        = number
  description = "The port number of the RDS instance."
  default     = 3306
}

variable "DB_NAME" {
  type        = string
  description = "The name of the database to connect to."
}

variable "DB_USER" {
  type        = string
  description = "The database username."
}

variable "DB_PASSWORD" {
  type        = string
  description = "The database password."
  sensitive   = true
}

variable "CUSTOM_AWS_REGION" {
  type        = string
  description = "The AWS region to use"
}

variable "API_KEY" {
  type        = string
  description = "The API key to store in Secrets Manager"
}

variable "API_SECRET" {
  type        = string
  description = "The API secret to store in Secrets Manager"
}

variable "SECRET_NAME" {
  type        = string
  description = "The name of the secret in AWS Secrets Manager"
}

variable "NOTIFICATION_EMAIL" {
  type        = string
  description = "The email address to receive SNS notifications"
}
