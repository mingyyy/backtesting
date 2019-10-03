# BackTesting

## Table of Contents

1. [Overview] (#overview)
2. [Project Ideas] (#project-ideas)
3. []

## Overview

In this document, I will outline the components of the project based on different ideas.
For each idea the structure will be:
1. Problem?
2. Solution?
3. Stack?


## Projects

### Project Idea 1
#### What's the problem?

When I was a risk manager in a private bank, I need to do stress test a few times a year for regulatory reasons,
and back testing to the methodology behind our multiple asset class collateral system. 
Two types of data sets I would need to complete this kind of task:
1. Portfolio of each client at time t (t1 -> tn)
2. Market data for the same period (stock prices, volatilities, interest rates, fx...)

With back testing, I need the historical data for certain period which was stored in tapes which needs to be 
retrieved and restored in a separate environment. The whole process takes a few days. Once, I finish with the task,
the resource is release and reassigned to other people. So the question is: would it be possible to get it cheaper and faster?
 

#### How can we solve this problem?

With readily available "Daily Historical Stock Prices (1970 - 2018)" -Historical stock prices for several thousand unique stock tickers (2G DATA) 
I could simulate some portfolios with different holdings. 
with different holdings of the coins and testing out one or two simple margin strategies. 

##### Data Ingestion
MVP: 
1. Raw Data
    - Download .zip from kaggle to local machine, unzip and upload to S3 bucket as .csv files.
    - Copy from othe's S3 bucket to my bucket ($aws s3 sync <path or s3url> to <path or s3 url: s3://huge-stock-data>)
      Source: https://registry.opendata.aws/deutsche-boerse-pds/
2. generate dummy trading strategy and test on those datasets python. pyspark (?)
    
    - same amount every n days (n from 1 to 90) on random stocks over all.
    - same amount every n days on 5 blue chips
    - same amount every n days on 10 small caps
    - random amount random days random stocks.
    - buy low and sell high random 10 stocks over all.
    - industry rotation

3. Pull data from S3 to Database Postgres (timesale DB? for time series)

4. web app to control the selection of different strategies (change of period, number of days, companies, industries etc).
django or flask (?) 

5. Build data lake with S3? partition? columnar?


Production:
Step 1. Generate larger dataset (python) in EC2.
    - Different exchanges based on % of each industry (smaller scale)
    - Different exchanges based on % of some industry (smaller scale and restricted)
    - Totally random
    - Interval of the daily between high and lows

Ingestion of historical datasets into S3 bucket 1 (one time) using PySpark.
~~Step 2. Streaming to S3 bucket 2 for incoming data (continuous)
Step 3. Daily or weekly merge cleaned bucket 2 to bucket 1 (regular)~~
Step 2. Design margin strategies (>=2) in Python
Step 3. 
Step 4. Light web app to control the whole ingestion process. (Flask or Django)

##### Data Testing
Step 1. 


### Project Idea - 2
#### What's the problem?

System migration is costly and painful. Regardless of the existing structure and technology used, there are certain aspects need to be taken care of.
It is likely that user need to move their historical data into cloud for either business or regulatory purposes. 
Assume that they want to upgrade to AWS S3 bucket to store their data, but they don't want to handle the whole process 
yet they would like to have some easy ways to get insight into it.

Is it possible to create some tools to help with that process?

#### Possible Solution
Create a web app for users to register and provide their AWS info (hash that info for security?).

Main Functionality of the app would be to handle various types of data with different frequencies.

1. Batch upload Excel, text, pdf etc files from web app to S3. In this tutorial, the author picked AWS Lambda to be the tool.
(https://medium.com/think-serverless/implementing-a-serverless-batch-file-processing-application-11cafd11f073)
I am considering use spark for the batch data ingestion. 

2. Providing batch merging for certain types of data. Interface to handle errors, make changes and re-upload the files. 
Possible error suggestions.

3. Create a data structure tree and information dashboard.

4. Test with different types of large files. 

5. Data streaming going forward for certain types of data.

6. Periodical data batch upload scheduled.

### Notes on setting the environment

1. Using pegasus to start a spark cluster on EC2 instances. In this case, there are 3 workers and 1 master all on m4.large 
Ubuntu 16.04 images.
2. Install all the necessary packages according to requirements.txt
3. Configure for Spark History Server. 
    - Useful links:
        https://www.ibm.com/support/knowledgecenter/en/SS3MQL_1.1.1/management_sym/spark_configuring_history_service.html
        
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