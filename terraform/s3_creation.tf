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

resource "aws_s3_object" "address_csv" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key    = "address/created_at.txt"
  source = "data/address.csv"
}

resource "aws_s3_object" "created_at_date_sales_order" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key    = "sales_order/created_at.txt"
  source = "data/created_at.txt"
}

resource "aws_s3_object" "sales_order_csv" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key    = "sales_order/created_at.txt"
  source = "data/sales_order.csv"
}

resource "aws_s3_object" "created_at_date_design" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key    = "design/created_at.txt"
  source = "data/created_at.txt"
}

resource "aws_s3_object" "design_csv" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key    = "design/created_at.txt"
  source = "data/design.csv"
}

resource "aws_s3_object" "created_at_date_currency" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key    = "currency/created_at.txt"
  source = "data/created_at.txt"
}

resource "aws_s3_object" "currency_csv" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key    = "currency/created_at.txt"
  source = "data/currency.csv"
}

resource "aws_s3_object" "created_at_date_counterparty" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key    = "counterparty/created_at.txt"
  source = "data/created_at.txt"
}

resource "aws_s3_object" "counterparty_csv" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key    = "counterparty/created_at.txt"
  source = "data/counterparty.csv"
}

resource "aws_s3_object" "created_at_date_department" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key    = "department/created_at.txt"
  source = "data/created_at.txt"
}

resource "aws_s3_object" "department_csv" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key    = "department/created_at.txt"
  source = "data/department.csv"
}

resource "aws_s3_object" "created_at_date_purchase_order" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key    = "purchase_order/created_at.txt"
  source = "data/created_at.txt"
}

resource "aws_s3_object" "purchase_order_csv" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key    = "purchase_order/created_at.txt"
  source = "data/purchase_order.csv"
}

resource "aws_s3_object" "created_at_date_payment" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key    = "payment/created_at.txt"
  source = "data/created_at.txt"
}

resource "aws_s3_object" "payment_csv" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key    = "payment/created_at.txt"
  source = "data/payment.csv"
}

resource "aws_s3_object" "created_at_date_transaction" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key    = "transaction/created_at.txt"
  source = "data/created_at.txt"
}

resource "aws_s3_object" "transaction_csv" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key    = "transaction/created_at.txt"
  source = "data/transaction.csv"
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
