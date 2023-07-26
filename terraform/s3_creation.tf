resource "aws_s3_bucket" "ingestion_bucket" {
    bucket = "kp-northcoder-ingestion-bucket"
}

resource "aws_s3_bucket" "data_bucket" {
    bucket = "kp-northcoders-data-bucket"
}

resource "aws_s3_object" "created_at_date_address" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key    = "created_at.txt"
  source = "data/created_at.txt"
}