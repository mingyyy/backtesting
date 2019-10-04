#!/usr/bin/env bash


scp -i ~/.ssh/my-IAM-keypair.pem /Users/Ming/PycharmProjects/Backtesting/db/schema.sql  ubuntu@ec2-3-229-236-236.compute-1.amazonaws.com:/home/ubuntu/


# create schema in DB EC2
sudo psql -U postgres -d postgres -a -f schema.sql