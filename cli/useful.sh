#!/usr/bin/env bash

# environment variables
export PYSPARK_PYTHON=python3;
export SPARK_CLASSPATH= <path to downloaded jar>/postgresql-42.2.8.jar

# spark shell
pyspark

# copy files to
scp -i ~/.ssh/my-IAM-keypair.pem <schema file>  ubuntu@<public DNS>:/home/ubuntu/


# create schema in DB EC2
sudo psql -U postgres -d postgres -a -f schema.sql


# check file sizes in AWS S3 bucket
aws s3 ls --summarize --human-readable --recursive s3://<bucket-name>/<directory>

# how to start history server in master node
cd /usr/local/spark/sbin &&  ./start-history-server.sh

# run spark from terminal
spark-submit --master spark://<private DNS>:7077  --jars <path to jar>/postgresql-42.2.8.jar  <path to file>

# copy to server
scp -i ~/.ssh/my-IAM-keypair.pem  <local path>   <server path>

