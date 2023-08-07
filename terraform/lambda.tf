resource "aws_lambda_function" "ingestion_lambda" {
  filename         = data.archive_file.ingestion_lambda_code.output_path
  function_name    = "ingestion-lambda"
  role             = aws_iam_role.lambda_role.arn
  handler          = "ingestion.lambda_handler"
  source_code_hash = data.archive_file.ingestion_lambda_code.output_base64sha256
  runtime          = "python3.9"
  layers           = [aws_lambda_layer_version.my_lambda_layer.arn]
  timeout          = 120
}

resource "aws_lambda_function" "remodelling_lambda" {
  filename         = data.archive_file.remodelling_lambda_code.output_path
  function_name    = "remodelling-lambda"
  role             = aws_iam_role.lambda_role.arn
  handler          = "remodelling.lambda_handler"
  source_code_hash = data.archive_file.remodelling_lambda_code.output_base64sha256
  runtime          = "python3.9"
  layers           = [aws_lambda_layer_version.my_lambda_layer.arn]
  timeout          = 120
}

# resource "aws_lambda_function" "loading_lambda" {
#   filename         = data.archive_file.loading_lambda_code.output_path
#   function_name    = "loading-lambda"
#   role             = aws_iam_role.lambda_role.arn
#   handler          = "remodelling.lambda_handler"
#   source_code_hash = data.archive_file.remodelling_lambda_code.output_base64sha256
#   runtime          = "python3.9"
#   layers           = [aws_lambda_layer_version.my_lambda_layer.arn]
# }

resource "aws_lambda_permission" "allow_s3" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.remodelling_lambda.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.ingestion_bucket.arn
  source_account = data.aws_caller_identity.current.account_id
}

# resource "aws_lambda_permission" "allow_s3" {
#   action         = "lambda:InvokeFunction"
#   function_name  = aws_lambda_function.loading_lambda.function_name
#   principal      = "s3.amazonaws.com"
#   source_arn     = aws_s3_bucket.processed_bucket.arn
#   source_account = data.aws_caller_identity.current.account_id
# }
