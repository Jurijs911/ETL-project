resource "aws_cloudwatch_log_group" "ingestion_log_group" {
  name = "/aws/lambda/ingestion-lambda"
}

resource "aws_cloudwatch_log_group" "remodelling_log_group" {
  name = "/aws/lambda/remodelling-lambda"
}

# resource "aws_cloudwatch_log_group" "loading_log_group" {
#   name = "/aws/lambda/loading-lambda"
# }
