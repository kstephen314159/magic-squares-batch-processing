terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# S3 Bucket for Magic Squares
resource "aws_s3_bucket" "m4_squares" {
  bucket        = "m4.squares.megadodo.umb"
  force_destroy = true

  tags = {
    Name = "m4.squares.megadodo.umb"
  }
}

# S3 Bucket Lifecycle Configuration
resource "aws_s3_bucket_lifecycle_configuration" "m4_squares_lifecycle" {
  bucket = aws_s3_bucket.m4_squares.id

  rule {
    id     = "expiration"
    status = "Enabled"

    filter {}

    expiration {
      days = 3
    }
  }
}

# CloudWatch Log Group for Firehose
resource "aws_cloudwatch_log_group" "firehose_log_group" {
  name              = "umb/megadodo/magic_square/m4"
  retention_in_days = 0
}

resource "aws_cloudwatch_log_stream" "firehose_log_stream" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.firehose_log_group.name
}

# IAM Role for Firehose
resource "aws_iam_role" "firehose_role" {
  name = "firehose-magic-square-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "firehose.amazonaws.com"
        }
      }
    ]
  })
}

# IAM Policy for Firehose to write to S3
resource "aws_iam_role_policy" "firehose_s3_policy" {
  name = "firehose-s3-policy"
  role = aws_iam_role.firehose_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:AbortMultipartUpload",
          "s3:GetBucketLocation",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:ListBucketMultipartUploads",
          "s3:PutObject"
        ]
        Resource = [
          aws_s3_bucket.m4_squares.arn,
          "${aws_s3_bucket.m4_squares.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:PutLogEvents"
        ]
        Resource = [
          aws_cloudwatch_log_group.firehose_log_group.arn,
          "${aws_cloudwatch_log_group.firehose_log_group.arn}:*"
        ]
      }
    ]
  })
}

# Kinesis Firehose Delivery Stream
resource "aws_kinesis_firehose_delivery_stream" "magic_square_stream" {
  name        = "magic-square-stream"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose_role.arn
    bucket_arn = aws_s3_bucket.m4_squares.arn
    prefix     = "partitions/"

    buffering_interval = 60
    buffering_size     = 1

    compression_format = "GZIP"

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose_log_group.name
      log_stream_name = aws_cloudwatch_log_stream.firehose_log_stream.name
    }
  }
}

# Athena Database
resource "aws_athena_database" "magic_squares" {
  name          = "magic_squares"
  bucket        = aws_s3_bucket.m4_squares.id
  force_destroy = true
  
  properties = {
    location = "s3://${aws_s3_bucket.m4_squares.bucket}/athena-metadata/"
  }
}

# Athena Workgroup
resource "aws_athena_workgroup" "magic_squares_workgroup" {
  name = "magic-squares-workgroup"

  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.m4_squares.bucket}/query-results/"
    }
  }
}
