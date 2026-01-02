output "s3_bucket_name" {
  description = "Name of the S3 bucket for magic squares"
  value       = aws_s3_bucket.m4_squares.id
}

output "s3_bucket_arn" {
  description = "ARN of the S3 bucket"
  value       = aws_s3_bucket.m4_squares.arn
}

output "firehose_stream_name" {
  description = "Name of the Kinesis Firehose delivery stream"
  value       = aws_kinesis_firehose_delivery_stream.magic_square_stream.name
}

output "firehose_stream_arn" {
  description = "ARN of the Kinesis Firehose delivery stream"
  value       = aws_kinesis_firehose_delivery_stream.magic_square_stream.arn
}

output "log_group_name" {
  description = "CloudWatch log group name"
  value       = aws_cloudwatch_log_group.firehose_log_group.name
}

output "athena_database_name" {
  description = "Athena database name"
  value       = aws_athena_database.magic_squares.name
}
