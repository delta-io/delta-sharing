variable "aws_region" {
  default = "us-east-1"
}

variable "s3_bucket_name" {
}

variable "ami_id" {
  default = "ami-069aabeee6f53e7bf"
  type    = string
}

variable "az1" {
  default = "us-east-1a"
  type    = string
}
