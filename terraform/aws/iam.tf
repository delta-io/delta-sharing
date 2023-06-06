# Define IAM role for EC2 instances
resource "aws_iam_role" "deltasharing_ec2_role" {
  name = "deltasharing-ec2-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Application = "DeltaSharing"
  }
}

# Define IAM policy to grant access to S3
resource "aws_iam_policy" "deltasharing_ec2_s3_policy" {
  name = "delta-sharing-ec2-s3-policy"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:Get*",
          "s3:List*"
        ]
        Resource = "*"
      }
    ]
  })
}

# Attach S3 policy to IAM role
resource "aws_iam_role_policy_attachment" "delta_sharing_ec2_s3_attachment" {
  policy_arn = aws_iam_policy.deltasharing_ec2_s3_policy.arn
  role       = aws_iam_role.deltasharing_ec2_role.name
}

resource "aws_vpc_endpoint" "s3_endpoint" {
  vpc_id          = aws_vpc.deltasharing.id
  service_name    = "com.amazonaws.us-east-1.s3"
  route_table_ids = [aws_route_table.deltasharing_route_table.id]
}

resource "aws_vpc_endpoint_route_table_association" "s3_endpoint_association" {
  route_table_id  = aws_route_table.deltasharing_route_table.id
  vpc_endpoint_id = aws_vpc_endpoint.s3_endpoint.id
}

resource "aws_iam_instance_profile" "deltasharing_instance_profile" {
  name = "deltasharing-instance-profile"

  role = aws_iam_role.deltasharing_ec2_role.name
}