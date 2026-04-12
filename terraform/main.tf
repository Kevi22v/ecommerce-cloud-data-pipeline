terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.23"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.4"
    }
  }
}
provider "aws" {
  region = "us-east-1"
}

# ==========================================
# 1. SECURE PASSWORD GENERATOR
# ==========================================
resource "random_password" "db_password" {
  length           = 16
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

# ==========================================
# 2. NETWORKING (VPC & Subnets)
# ==========================================
resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags = { Name = "ecommerce-secure-vpc" }
}

resource "aws_subnet" "public_1" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "us-east-1a"
  map_public_ip_on_launch = true
  tags = { Name = "ecommerce-public-subnet-1" }
}

resource "aws_subnet" "public_2" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.2.0/24"
  availability_zone       = "us-east-1b"
  map_public_ip_on_launch = true
  tags = { Name = "ecommerce-public-subnet-2" }
}

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.main.id
}

resource "aws_route_table" "rt" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }
}

resource "aws_route_table_association" "a1" {
  subnet_id      = aws_subnet.public_1.id
  route_table_id = aws_route_table.rt.id
}
resource "aws_route_table_association" "a2" {
  subnet_id      = aws_subnet.public_2.id
  route_table_id = aws_route_table.rt.id
}

# ==========================================
# 3. DATABASE (RDS PostgreSQL - LOCKED DOWN)
# ==========================================
resource "aws_security_group" "db_sg" {
  name        = "ecommerce-secure-db-sg"
  vpc_id      = aws_vpc.main.id
  
  ingress {
    description = "Allow Postgres traffic ONLY from inside the VPC"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.main.cidr_block] # Security Upgrade: Replaced 0.0.0.0/0
  }
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_db_subnet_group" "db_subnet" {
  name       = "ecommerce-secure-db-subnet"
  subnet_ids = [aws_subnet.public_1.id, aws_subnet.public_2.id]
}

resource "aws_db_instance" "postgres" {
  identifier             = "ecommerce-secure-db"
  engine                 = "postgres"
  engine_version         = "15"
  instance_class         = "db.t3.micro"
  allocated_storage      = 20
  db_name                = "ecommerce_db"
  username               = "dbadmin"
  password               = random_password.db_password.result # Security Upgrade: Dynamic Password
  skip_final_snapshot    = true
  publicly_accessible    = false # Security Upgrade: Invisible to the public internet
  vpc_security_group_ids = [aws_security_group.db_sg.id]
  db_subnet_group_name   = aws_db_subnet_group.db_subnet.name
}

# ==========================================
# 4. KUBERNETES CLUSTER (EKS)
# ==========================================
resource "aws_iam_role" "eks_cluster" {
  name = "ecommerce-eks-cluster-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Action = "sts:AssumeRole", Effect = "Allow", Principal = { Service = "eks.amazonaws.com" } }]
  })
}

resource "aws_iam_role_policy_attachment" "eks_cluster_policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
  role       = aws_iam_role.eks_cluster.name
}

resource "aws_eks_cluster" "eks" {
  name     = "ecommerce-cluster"
  role_arn = aws_iam_role.eks_cluster.arn
  vpc_config {
    subnet_ids = [aws_subnet.public_1.id, aws_subnet.public_2.id]
  }
  depends_on = [aws_iam_role_policy_attachment.eks_cluster_policy]
}

# ==========================================
# 5. EKS WORKER NODES (Auto-Scaling Enabled)
# ==========================================
resource "aws_iam_role" "eks_nodes" {
  name = "ecommerce-eks-node-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Action = "sts:AssumeRole", Effect = "Allow", Principal = { Service = "ec2.amazonaws.com" } }]
  })
}

resource "aws_iam_role_policy_attachment" "nodes_policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
  role       = aws_iam_role.eks_nodes.name
}
resource "aws_iam_role_policy_attachment" "nodes_cni" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
  role       = aws_iam_role.eks_nodes.name
}
resource "aws_iam_role_policy_attachment" "nodes_ecr" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
  role       = aws_iam_role.eks_nodes.name
}
resource "aws_iam_role_policy_attachment" "nodes_s3_access" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
  role       = aws_iam_role.eks_nodes.name
}

resource "aws_iam_role_policy" "cluster_autoscaler_policy" {
  name = "ecommerce-cluster-autoscaler-policy"
  role = aws_iam_role.eks_nodes.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "autoscaling:DescribeAutoScalingGroups",
          "autoscaling:DescribeAutoScalingInstances",
          "autoscaling:DescribeLaunchConfigurations",
          "autoscaling:DescribeTags",
          "autoscaling:SetDesiredCapacity",
          "autoscaling:TerminateInstanceInAutoScalingGroup",
          "ec2:DescribeLaunchTemplateVersions"
        ]
        Effect   = "Allow"
        Resource = "*"
      },
    ]
  })
}

resource "aws_eks_node_group" "node_group" {
  cluster_name    = aws_eks_cluster.eks.name
  node_group_name = "ecommerce-node-group"
  node_role_arn   = aws_iam_role.eks_nodes.arn
  subnet_ids      = [aws_subnet.public_1.id, aws_subnet.public_2.id]
  instance_types  = ["t3.medium"] 

  scaling_config {
    desired_size = 2
    min_size     = 2
    max_size     = 4
  }
  tags = {
    "k8s.io/cluster-autoscaler/ecommerce-cluster" = "owned"
    "k8s.io/cluster-autoscaler/enabled"           = "true"
  }

  depends_on = [
    aws_iam_role_policy_attachment.nodes_policy,
    aws_iam_role_policy_attachment.nodes_cni,
    aws_iam_role_policy_attachment.nodes_ecr
  ]
}

# ==========================================
# OUTPUTS
# ==========================================
output "rds_endpoint" {
  value       = aws_db_instance.postgres.endpoint
  description = "The internal URL for the Postgres Database"
}

output "eks_cluster_name" {
  value = aws_eks_cluster.eks.name
}

# We output the password so YOU can see it, but mark it sensitive so it doesn't print on the screen normally
output "db_password" {
  value       = random_password.db_password.result
  sensitive   = true
  description = "The secure, randomly generated database password"
}

# Generate a random string to ensure the bucket name is globally unique
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# The Core S3 Data Lake Bucket
resource "aws_s3_bucket" "data_lake" {
  bucket        = "ecommerce-datalake-${random_id.bucket_suffix.hex}"
  
  force_destroy = true 

  tags = {
    Name        = "Ecommerce Data Lake"
    Environment = "Capstone"
  }
}

# Enable Versioning
resource "aws_s3_bucket_versioning" "data_lake_versioning" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# This guarantees nobody on the public internet can read your Data Lake
resource "aws_s3_bucket_public_access_block" "data_lake_security" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Output the final bucket name
output "data_lake_bucket_name" {
  value       = aws_s3_bucket.data_lake.bucket
  description = "The globally unique name of the S3 Data Lake"
}

# ==========================================
# 6.5. IAM USER FOR SPARK S3 ACCESS
# ==========================================
resource "aws_iam_user" "processor_user" {
  name = "ecommerce-processor-user"
}

resource "aws_iam_user_policy_attachment" "processor_s3" {
  user       = aws_iam_user.processor_user.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_access_key" "processor_key" {
  user = aws_iam_user.processor_user.name
}

# ==========================================
# 7. ZERO-TOUCH KUBERNETES AUTOMATION
# ==========================================
# Connect Terraform to your EKS Cluster
provider "kubernetes" {
  host                   = aws_eks_cluster.eks.endpoint
  cluster_ca_certificate = base64decode(aws_eks_cluster.eks.certificate_authority[0].data)
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    args        = ["eks", "get-token", "--cluster-name", aws_eks_cluster.eks.name]
    command     = "aws"
  }
}

# Auto-Inject Secret into Kubernetes
resource "kubernetes_secret" "ecommerce_secrets" {
  metadata { name = "ecommerce-secrets" }
  data = { 
    DB_PASSWORD    = random_password.db_password.result
    AWS_ACCESS_KEY = aws_iam_access_key.processor_key.id
    AWS_SECRET_KEY = aws_iam_access_key.processor_key.secret
  }
}

# Auto-Inject ConfigMap into Kubernetes
resource "kubernetes_config_map" "ecommerce_config" {
  metadata { name = "ecommerce-config" }
  data = {
    DB_HOST      = split(":", aws_db_instance.postgres.endpoint)[0] 
    DB_NAME      = "ecommerce_db"
    DB_USER      = "dbadmin"
    S3_BUCKET    = aws_s3_bucket.data_lake.bucket
    KAFKA_BROKER = "kafka-service:9092"
    KAFKA_TOPIC  = "ecommerce_orders"
  }
}

# ==========================================
# 8. EVALUATION FILES (Auto-Generating the YAMLs)
# ==========================================
# This writes the secret.yaml file to your local computer
resource "local_file" "secret_yaml" {
  filename = "${path.module}/../k8s/secret.yaml"
  content  = <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: ecommerce-secrets
type: Opaque
stringData:
  DB_PASSWORD: "${random_password.db_password.result}"
EOF
}

# This writes the configmap.yaml file to your local computer
resource "local_file" "configmap_yaml" {
  filename = "${path.module}/../k8s/configmap.yaml"
  content  = <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: ecommerce-config
data:
  DB_HOST: "${split(":", aws_db_instance.postgres.endpoint)[0]}"
  DB_NAME: "ecommerce_db"
  DB_USER: "dbadmin"
  S3_BUCKET: "${aws_s3_bucket.data_lake.bucket}"
  KAFKA_BROKER: "kafka-service:9092"
  KAFKA_TOPIC: "ecommerce_orders"
EOF
}

# ==========================================
# 9. AUTO-UPDATE LOCAL KUBECONFIG
# ==========================================
resource "null_resource" "update_kubeconfig" {
  # This triggers every time the EKS cluster changes
  triggers = {
    cluster_name = aws_eks_cluster.eks.name
    endpoint     = aws_eks_cluster.eks.endpoint
  }

  # This runs the exact terminal command on your local laptop!
  provisioner "local-exec" {
    command = "aws eks update-kubeconfig --region us-east-1 --name ${aws_eks_cluster.eks.name}"
  }

  depends_on = [aws_eks_cluster.eks]
}