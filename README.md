# Back In Time

## Table of Contents

1. [Motivation](#motivation)
2. [Overview](#overview)
3. [Pipeline](#pipeline)

## Motivation
In my previous jobs, I have seen the same problems with data management over and over again, 
across different countries and industries. Often, employees work on some Excel files 
and save them in their local machine or shared drive on a daily basis. 
Over the time and possibly several rounds of staff turn-overs,
the daily files become unmanageable. Besides security and cost concerns, 
it is almost impossible to use those historical data efficiently. 

To address this ubiquitous problem, I build a pipeline that will help companies bettter manage their historical data, 
so the security, cost and efficiency issues could be addressed properly.


## Overview
The main idea of this project is to build a pipeline that helps business to better handle their historical files.
![pipeline](https://media.giphy.com/media/XRB1uf2F9bGOA/giphy.gif)
Therefore, I am showing how to migrate large amount of csv files, close to 2000 csv files and about 437GB in total, of different sizes to AWS S3 bucket.




 


In this document, I walk through each step of the project based on the workflow.
1. Generation of csv files under folder simulation
2. Conversion to Parquet files under folder spark
3. Transformation in Spark under folder spark
4. Results in Database
5. UI in Dash under folder dash

The structure of the directory are mapped according to this tree:
```
backtesting
    |- api
        |- api_18080.py
    |- cli
        |- useful.sh
    |- comparison
        |-
    |- dash
        |- app_dash.py
    |- db
        |- schema.sql
    |- python
        |- connect_s3.py
        |- create_table.py
    |- simulation
        |- GBM.py
        |- merton_jump.py
        |- dates_generator.py
        |- price_generator_final.py
    |- spark
        |- field_detector.py
        |- file_convertor.py
        |- strategy.py
```

### Pipeline
Insert graph


### Dataset
Simulation

Why csv files?

Since MS Excel is [arguably the most important computer program in workplace around](https://www.investopedia.com/articles/personal-finance/032415/importance-excel-business.asp) the world. 


Focusing on how to handle large amount of different sizes of csv files seems to be a good choice.
Step 1. Generate larger dataset (python) in EC2.
    - Different exchanges based on % of each industry (smaller scale)
    - Different exchanges based on % of some industry (smaller scale and restricted)
    - Totally random
    - Interval of the daily between high and lows

Ingestion of historical datasets into S3 bucket 1 (one time) using PySpark.

### Conversion

### Transformation

### Database

### Visualization

### Future
Insert graph here



### Notes on setting the environment

1. Following this [pegasus](https://blog.insightdatascience.com/how-to-get-hadoop-and-spark-up-and-running-on-aws-7a1b0ab55459) 
instruction to start a spark cluster on EC2 instances. In this case, there are 3 workers and 1 master all on m4.large 
Ubuntu 16.04 images.
2. Install all the necessary packages according to requirements.txt
3. Configure for Spark History Server. 
    - Useful [link](https://www.ibm.com/support/knowledgecenter/en/SS3MQL_1.1.1/management_sym/spark_configuring_history_service.html)
     on how to configure spark history server.
        
    - Modify spark-defaults.conf file under directory /usr/local/spark/conf. In this case, a folder called spark-events is created under /tmp/
    that will be used for storage of the history logs.
        ```
        spark.eventLog.enabled         true
        spark.eventLog.dir             file:///tmp/spark-events
        spark.history.fs.logDirectory  file:///tmp/spark-events
        ```
    - Start the spark service
        ```
        $cd /usr/local/spark/sbin/ && sudo ./start-history-server.sh
        ```
    - If it is a success, you should see this on your screen:
        ```
        starting org.apache.spark.deploy.history.HistoryServer, logging to /usr/local/spark/logs/spark-root-org.apache.spark.deploy.history.HistoryServer-1-ip-10-0-0-6.out
        ```
    - And now WebUI with port 18080 should work
4. Configure for connecting to Postgres Database, in this case M4.Large EC2 instance, 16.04 Ubuntu image.
