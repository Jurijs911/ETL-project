resource "aws_s3_bucket" "processed_bucket" {
  bucket = "kp-northcoders-processed-bucket"
}

resource "aws_s3_object" "sales_order_txt" {
  bucket = aws_s3_bucket.processed_bucket.bucket
  key    = "sales_order/last_processed.txt"
  source = "data/last_processed.txt"
}

resource "aws_s3_object" "fact_sales_order_csv" {
  bucket = aws_s3_bucket.processed_bucket.bucket
  key    = "fact_sales_order.csv"
  source = "data/remodelling/fact_sales_order.csv"
}

resource "aws_s3_object" "design_txt" {
  bucket = aws_s3_bucket.processed_bucket.bucket
  key    = "design/last_processed.txt"
  source = "data/last_processed.txt"
}

resource "aws_s3_object" "dim_design_csv" {
  bucket = aws_s3_bucket.processed_bucket.bucket
  key    = "dim_design.csv"
  source = "data/remodelling/dim_design.csv"
}

resource "aws_s3_object" "staff_txt" {
  bucket = aws_s3_bucket.processed_bucket.bucket
  key    = "staff/last_processed.txt"
  source = "data/last_processed.txt"
}

resource "aws_s3_object" "dim_staff_csv" {
  bucket = aws_s3_bucket.processed_bucket.bucket
  key    = "dim_staff.csv"
  source = "data/remodelling/dim_staff.csv"
}

resource "aws_s3_object" "address_txt" {
  bucket = aws_s3_bucket.processed_bucket.bucket
  key    = "address/last_processed.txt"
  source = "data/last_processed.txt"
}

resource "aws_s3_object" "dim_location_csv" {
  bucket = aws_s3_bucket.processed_bucket.bucket
  key    = "dim_location.csv"
  source = "data/remodelling/dim_location.csv"
}

resource "aws_s3_object" "currency_txt" {
  bucket = aws_s3_bucket.processed_bucket.bucket
  key    = "currency/last_processed.txt"
  source = "data/last_processed.txt"
}

resource "aws_s3_object" "dim_currency_csv" {
  bucket = aws_s3_bucket.processed_bucket.bucket
  key    = "dim_currency.csv"
  source = "data/remodelling/dim_currency.csv"
}

resource "aws_s3_object" "counterparty_txt" {
  bucket = aws_s3_bucket.processed_bucket.bucket
  key    = "counterparty/last_processed.txt"
  source = "data/last_processed.txt"
}

resource "aws_s3_object" "dim_counterparty_csv" {
  bucket = aws_s3_bucket.processed_bucket.bucket
  key    = "dim_counterparty.csv"
  source = "data/remodelling/dim_counterparty.csv"
}

resource "aws_s3_object" "department_txt" {
  bucket = aws_s3_bucket.processed_bucket.bucket
  key    = "department/last_processed.txt"
  source = "data/last_processed.txt"
}

resource "aws_s3_object" "dim_date_csv" {
  bucket = aws_s3_bucket.processed_bucket.bucket
  key    = "dim_date.csv"
  source = "data/remodelling/dim_date.csv"
}

resource "aws_s3_bucket_notification" "ingestion_bucket_notification" {
  bucket = aws_s3_bucket.ingestion_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.remodelling_lambda.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_s3]
}

# resource "aws_s3_bucket_notification" "processed_bucket_notification" {
#   bucket = aws_s3_bucket.processed_bucket.id

#   lambda_function {
#     lambda_function_arn = aws_lambda_function.loading_lambda.arn
#     events              = ["s3:ObjectCreated:*"]
#   }

#   depends_on = [aws_lambda_permission.allow_s3]
# }
