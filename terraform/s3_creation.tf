resource "aws_s3_bucket" "ingestion_bucket" {
  bucket = "kp-northcoders-ingestion-bucket"
}

resource "aws_s3_bucket" "processed_bucket" {
  bucket = "kp-northcoders-processed-bucket"
}

resource "aws_s3_object" "created_at_date_address" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key    = "address/created_at.txt"
  source = "data/created_at.txt"
}

resource "aws_s3_object" "created_at_date_sales_order" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key    = "sales_order/created_at.txt"
  source = "data/created_at.txt"
}

resource "aws_s3_object" "created_at_date_design" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key    = "design/created_at.txt"
  source = "data/created_at.txt"
}

resource "aws_s3_object" "created_at_date_currency" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key    = "currency/created_at.txt"
  source = "data/created_at.txt"
}

resource "aws_s3_object" "created_at_date_counterparty" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key    = "counterparty/created_at.txt"
  source = "data/created_at.txt"
}

resource "aws_s3_object" "created_at_date_department" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key    = "department/created_at.txt"
  source = "data/created_at.txt"
}

resource "aws_s3_object" "created_at_date_purchase_order" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key    = "purchase_order/created_at.txt"
  source = "data/created_at.txt"
}

resource "aws_s3_object" "created_at_date_payment" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key    = "payment/created_at.txt"
  source = "data/created_at.txt"
}

resource "aws_s3_object" "created_at_date_transaction" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key    = "transaction/created_at.txt"
  source = "data/created_at.txt"
}

resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.data_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.ingestion_bucket.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_s3]
}
