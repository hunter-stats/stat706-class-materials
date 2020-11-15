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

variable "db_port" {
  description = "database port"
  type        = number
  default     = 5432
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

data "template_file" "bastion_config" {
  template = file("./bastion_config.sh")
  vars = {
    kaggle_credentials = jsonencode(file("~/Downloads/kaggle.json"))
    DB_USER            = var.db_username
    DB_PASS            = var.db_password
    DB_PORT            = var.db_port
    DB_HOST            = aws_db_instance.database.address
  }
}

resource "aws_instance" "bastion" {
  ami                    = "ami-0d5d1a3aa3516231f"
  instance_type          = "t2.large"
  vpc_security_group_ids = [aws_security_group.bastion.id]
  key_name               = "${var.keypair_name}"
  user_data              = data.template_file.bastion_config.rendered

  tags = {
    Name = "stat-706-bastion"
  }
}

# database
resource "aws_security_group" "database" {
  name = "db_sg"

  ingress {
    from_port       = 0
    to_port         = var.db_port
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
  skip_final_snapshot    = true
}

output "public_ip" {
  value       = aws_instance.bastion.public_ip
  description = "the public ip address of the deployed ec2 instance"
}
