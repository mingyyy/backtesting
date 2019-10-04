#!/usr/bin/env bash
# !/bin/bash

export PYSPARK_PYTHON=python3;
export SPARK_CLASSPATH= <path to downloaded jar>/postgresql-42.2.8.jar

pyspark
spark-submit --driver-class-path <path to downloaded jar>/postgresql-42.2.8.jar  <file_name>.py
