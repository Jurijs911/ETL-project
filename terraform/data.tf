data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

# data "archive_file" "ingestion_lambda_code" {
#   type        = "zip"
#   output_path = "${path.module}/../ingestion.zip"
#   source_dir  = "${path.module}/../src/ingestion"
#   depends_on  = [null_resource.install_dependencies]
# }

data "archive_file" "remodelling_lambda_code" {
  type        = "zip"
  output_path = "${path.module}/../remodelling.zip"
  source_dir  = "${path.module}/../src/remodelling"
  depends_on  = [null_resource.install_dependencies]
}

# data "archive_file" "loading_lambda_code" {
#   type        = "zip"
#   output_path = "${path.module}/../loading.zip"
#   source_dir  = "${path.module}/../src/loading"
#   depends_on  = [null_resource.install_dependencies]
# }
