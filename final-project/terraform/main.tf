provider "aws" {
  region = "us-west-2"
}

# variables
variable "keypair_name" {
  description = "the name of the ssh key used to access the ec2 instances"
  type        = string
  default     = "nickhil_sethi_ec2_ssh"
}

variable "db_username" {
  description = "the username for the database"
  type        = string
  default     = "postgres"
}

variable "db_password" {
  description = "the password for the database"
  type        = string
}

# bastion
resource "aws_security_group" "bastion" {
  name = "terraform-example-instance"

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "bastion" {
  ami                    = "ami-0d5d1a3aa3516231f"
  instance_type          = "t2.large"
  vpc_security_group_ids = [aws_security_group.bastion.id]
  key_name               = "${var.keypair_name}"
  user_data              = file("./bastion_config.sh")

  provisioner "file" {
    source      = "~/.kaggle/kaggle.json"
    destination = "/home/ec2-user/kaggle.json"
  }
  
  tags = {
    Name = "stat-706-bastion"
  }
}

# database
resource "aws_security_group" "database" {
  name = "db_sg"

  ingress {
    from_port       = 0
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.bastion.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "stat-706-db-sg"
  }
}
resource "aws_db_instance" "database" {
  allocated_storage      = 20
  storage_type           = "gp2"
  engine                 = "postgres"
  instance_class         = "db.t2.large"
  name                   = "stat_706_db"
  username               = var.db_username
  password               = var.db_password
  vpc_security_group_ids = [aws_security_group.database.id]
}

output "public_ip" {
  value       = aws_instance.bastion.public_ip
  description = "the public ip address of the deployed ec2 instance"
}
