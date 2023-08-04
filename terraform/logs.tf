resource "aws_cloudwatch_log_group" "ingestion_log_group" {
  name = "/aws/lambda/ingestion-lambda"
}

resource "aws_cloudwatch_log_stream" "ingestion_log_stream" {
  name           = "lambda-log-stream"
  log_group_name = aws_cloudwatch_log_group.ingestion_log_group.name
}

resource "aws_cloudwatch_log_group" "remodelling_log_group" {
  name = "/aws/lambda/remodelling-lambda"
}

resource "aws_cloudwatch_log_stream" "remodelling_log_stream" {
  name           = "lambda-log-stream"
  log_group_name = aws_cloudwatch_log_group.remodelling_log_group.name
}

# resource "aws_cloudwatch_log_group" "loading_log_group" {
#   name = "/aws/lambda/loading-lambda"
# }

# resource "aws_cloudwatch_log_stream" "ingestion_log_stream" {
#   name           = "lambda-log-stream"
#   log_group_name = aws_cloudwatch_log_group.ingestion_log_group.name
# }
