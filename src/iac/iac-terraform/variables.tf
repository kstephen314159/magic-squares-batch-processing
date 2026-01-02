variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "glue_database_name" {
  description = "AWS Glue database name for Parquet schema"
  type        = string
  default     = ""
}

variable "glue_table_name" {
  description = "AWS Glue table name for Parquet schema"
  type        = string
  default     = ""
}
