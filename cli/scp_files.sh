#!/usr/bin/env bash


scp -i ~/.ssh/my-IAM-keypair.pem /Users/Ming/PycharmProjects/Backtesting/secrete.py ubuntu@ec2-3-228-208-95.compute-1.amazonaws.com:/home/ubuntu/insight/backtesting
