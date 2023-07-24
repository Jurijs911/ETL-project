resource "aws_s3_bucket" "ingestion_bucket" {
    bucket = "kp-northcoder-ingestion-bucket"
}

resource "aws_s3_bucket" "data_bucket" {
    bucket = "kp-northcoders-data-bucket"
}