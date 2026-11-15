terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = "us-east-2"
}

variable "bucket_name" {
  description = "Existing bucket holding the prediction files. Referenced, not managed."
  type        = string
}

variable "prefix" {
  description = "Folder pattern Airflow may read. daily_* covers daily_2026/, daily_2027/, ..."
  type        = string
  default     = "daily_"
}

# The identity Airflow logs in as. Its only permission: assume the reader role.
resource "aws_iam_user" "airflow" {
  name = "airflow-predictions"
}

resource "aws_iam_user_policy" "assume_reader_only" {
  user = aws_iam_user.airflow.name
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sts:AssumeRole", 
    Resource = aws_iam_role.reader.arn }]
  })
}

# The role that can read, and only the daily_* folders in one bucket.
resource "aws_iam_role" "reader" {
  name = "predictions-reader"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { AWS = aws_iam_user.airflow.arn }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "read_predictions" {
  role = aws_iam_role.reader.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Action    = "s3:ListBucket"
        Resource  = "arn:aws:s3:::${var.bucket_name}"
        Condition = { StringLike = { "s3:prefix" = ["${var.prefix}*"] } }
      },
      {
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "arn:aws:s3:::${var.bucket_name}/${var.prefix}*"
      }
    ]
  })
}

output "reader_role_arn" {
  value = aws_iam_role.reader.arn
}