locals {
  layer_zip_path    = "layer.zip"
  layer_name        = "my_lambda_requirements_layer"
  requirements_path = "${path.module}/../requirements.txt"
}

resource "null_resource" "lambda_layer" {
  triggers = {
    requirements = filesha1(local.requirements_path)
  }
  # the command to install python and dependencies to the machine and zips
  provisioner "local-exec" {
    command = <<EOT
      set -e
      sudo apt-get update
      sudo apt install python3 python3-pip zip -y
      rm -rf python
      mkdir python
      sudo pip3 install -r ${local.requirements_path} -t python/
      sudo zip -r ${local.layer_zip_path} python/
    EOT
  }
}

resource "aws_s3_bucket" "lambda_layer_bucket" {
  bucket = "kp-northcoders-dependency-bucket"
}

resource "aws_s3_object" "lambda_layer_zip" {
  bucket     = aws_s3_bucket.lambda_layer_bucket.id
  key        = "lambda_layers/${local.layer_name}/${local.layer_zip_path}"
  source     = local.layer_zip_path
  depends_on = [null_resource.lambda_layer] # triggered only if the zip file is created
}

resource "aws_lambda_layer_version" "my_lambda_layer" {
  s3_bucket           = aws_s3_bucket.lambda_layer_bucket.id
  s3_key              = aws_s3_object.lambda_layer_zip.key
  layer_name          = local.layer_name
  compatible_runtimes = ["python3.9"]
  skip_destroy        = true
  depends_on          = [aws_s3_object.lambda_layer_zip] # triggered only if the zip file is uploaded to the bucket
}
