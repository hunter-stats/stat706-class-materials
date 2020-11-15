terraform {
  backend "s3" {
    bucket = "stat706-final-project-tf-state"
    key    = "global/s3/terraform.tfstate"
    region = "us-west-2"

    dynamodb_table = "stat706-final-project-tf-lock"
    encrypt        = true
  }
}
