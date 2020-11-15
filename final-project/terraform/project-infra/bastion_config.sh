#!/bin/bash

sudo yum install -y postgresql git python3 python3-wheel python-pip
pip install kaggle
mkdir /home/ec2-user/.kaggle
touch /home/ec2-user/.kaggle/kaggle.json
echo ${kaggle_credentials} > /home/ec2-user/.kaggle/kaggle.json
