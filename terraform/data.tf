data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

# data "archive_file" "ingestion_lambda_code" {
#   type        = "zip"
#   source_file = "${path.module}/../src/file_reader/reader.py" # UPDATE PATH
#   output_path = "${path.module}/../function.zip"              # UPDATE PATH
# }

data "archive_file" "remodelling_lambda_code" {
  type        = "zip"
  output_path = "${path.module}/../remodelling.zip" # UPDATE PATH
  source_dir  = "${path.module}/../src/remodelling"
  depends_on  = [null_resource.install_dependencies]
  excludes = ["${path.module}/../src/remodelling/package"]
}

# data "archive_file" "loading_lambda_code" {
#   type        = "zip"
#   source_file = "${path.module}/../src/file_reader/reader.py" # UPDATE PATH
#   output_path = "${path.module}/../function.zip"              # UPDATE PATH
# }
