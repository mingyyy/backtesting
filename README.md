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

With two ready datasets on Kaggle, bitcoin-blockchain (https://www.kaggle.com/bigquery/bitcoin-blockchain) 
and ethereum-blockchain (https://www.kaggle.com/bigquery/ethereum-blockchain), I could simulate some portfolios 
with different holdings of the coins and testing out one or two simple trading strategies. 

Step 1. Ingestion of historical datasets into S3
Step 2. 


### Project Idea - 2
#### What's the problem?

System migration is costly and painful. Regardless of the existing structure and technology used, there are certain aspects need to be taken care of,
is it possible to create some tools to help with that process?


### Project Idea - 3
#### What's the problem?