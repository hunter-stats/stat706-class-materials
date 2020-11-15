#!/bin/bash

# install system deps
sudo yum install -y postgresql git python3 python3-wheel python-pip gcc

# install pip deps
pip install kaggle
pip install pandas
pip install psycopg2-binary

# write kaggle creds
mkdir /home/ec2-user/.kaggle
touch /home/ec2-user/.kaggle/kaggle.json
echo ${kaggle_credentials} > /home/ec2-user/.kaggle/kaggle.json

# download data
kaggle datasets download -d rounakbanik/the-movies-dataset
unzip the-movies-dataset.zip

# clone the repo
git clone https://github.com/Nickhil-Sethi/stat706-class-materials.git
