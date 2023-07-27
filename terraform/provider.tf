terraform {
  backend "s3" {
    bucket = "terraform-backend-bucket-key-pears"
    key    = "terraform.tfstate"
    region = "eu-west-2"
  }
}

provider "aws" {
  region = "eu-west-2"
}
