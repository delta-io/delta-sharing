provider "aws" {
  region = var.aws_region
}

# AWS VPC
resource "aws_vpc" "deltasharing" {
  cidr_block = "10.0.0.0/16"

  tags = {
    Name        = "DeltaSharingNetwork"
    Description = "Delta Sharing network."
  }
}

# AWS Subnet
resource "aws_subnet" "deltasharing" {
  vpc_id                  = aws_vpc.deltasharing.id
  cidr_block              = "10.0.2.0/24"
  availability_zone       = var.az1
  map_public_ip_on_launch = false

  tags = {
    Name        = "internal"
    Description = "Internal subnet"
  }
}

# Create a route table
resource "aws_route_table" "deltasharing_route_table" {
  vpc_id = aws_vpc.deltasharing.id

  tags = {
    Name = "deltasharing-route-table"
  }
}

# Create an internet gateway
resource "aws_internet_gateway" "deltasharing_igw" {
  vpc_id = aws_vpc.deltasharing.id

  tags = {
    Name = "deltasharing-igw"
  }
}

# Create a route to the internet gateway in the route table
resource "aws_route" "deltasharing_route" {
  route_table_id         = aws_route_table.deltasharing_route_table.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.deltasharing_igw.id
}

# Associate subnet with the route table
resource "aws_route_table_association" "deltasharing_subnet_association" {
  subnet_id      = aws_subnet.deltasharing.id
  route_table_id = aws_route_table.deltasharing_route_table.id
}

# Delta Sharing security group
resource "aws_security_group" "deltasharing" {
  name        = "DeltaSharingSecurityGroup"
  description = "Delta Sharing security group"

  vpc_id = aws_vpc.deltasharing.id

  #ssh
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  #allow port 8080 within the VPC
  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.deltasharing.cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "all"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "DeltaSharingSecurityGroup"
    Description = "Delta Sharing security group."
  }
}

# AWS S3 Bucket
resource "aws_s3_bucket" "deltasharing" {
  bucket = var.s3_bucket_name

  tags = {
    Name        = "DeltaSharingStorageAccount"
    Description = "Delta Sharing storage account."
  }
}

resource "aws_s3_bucket_public_access_block" "deltasharing" {
  bucket = aws_s3_bucket.deltasharing.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# AWS EC2 Instance
resource "aws_instance" "deltasharing" {
  ami           = var.ami_id
  instance_type = "t2.medium"

  iam_instance_profile   = aws_iam_instance_profile.deltasharing_instance_profile.name
  vpc_security_group_ids = [aws_security_group.deltasharing.id]

  subnet_id = aws_subnet.deltasharing.id

  user_data = <<EOF
    #!/bin/bash
    cd /home/ec2-user && wget https://github.com/delta-io/delta-sharing/releases/download/v0.5.2/delta-sharing-server-0.5.2.zip
    unzip delta-sharing-server-0.5.2.zip
    EOF

  tags = {
    Name        = "DeltaSharingServer"
    Description = "Delta Sharing server"
  }

  root_block_device {
    volume_type           = "gp2"
    volume_size           = 30
    delete_on_termination = true
  }

  ebs_block_device {
    device_name           = "/dev/sdb"
    volume_type           = "gp2"
    volume_size           = 100
    delete_on_termination = true
  }
}

# create EIP to allow ssh access to the server
resource "aws_eip" "delta_sharing_eip" {
  vpc = true
}


resource "aws_eip_association" "eip_delta_assoc" {
  instance_id   = aws_instance.deltasharing.id
  allocation_id = aws_eip.delta_sharing_eip.id
  depends_on    = [aws_eip.delta_sharing_eip]
}


