# Generate a random string to ensure the bucket name is globally unique
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# The Core S3 Data Lake Bucket
resource "aws_s3_bucket" "data_lake" {
  bucket        = "ecommerce-datalake-${random_id.bucket_suffix.hex}"
  
  force_destroy = true 

  tags = {
    Name        = "Ecommerce Data Lake"
    Environment = "Capstone"
  }
}

# Enable Versioning
resource "aws_s3_bucket_versioning" "data_lake_versioning" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# This guarantees nobody on the public internet can read your Data Lake
resource "aws_s3_bucket_public_access_block" "data_lake_security" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Output the final bucket name
output "data_lake_bucket_name" {
  value       = aws_s3_bucket.data_lake.bucket
  description = "The globally unique name of the S3 Data Lake"
}