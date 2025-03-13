# Report Generation with parallel execution of Stored Procedures 
## Overview
This repository contains several stored procedures created to accept parameters and execute in a parallel manner on partitioned tables.

# Table of Contents
- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Execution Flow](#execution-flow)


## Introduction <a name="introduction"></a>
The report generation process consist of procedures and a Python Script. 
The Python script connects to a PostgreSQL database and utilize Python's multi-threading capabilities to execute some of the procedures concurrently and the remaining sequentially.

## Prerequisites <a name="prerequisites"></a>
Before running this report generation process, the following prerequisite must be meant.

- Installed Python 3.x
- PostgreSQL database with appropriate partitioned tables
- psycopg2 library (pip install psycopg2)
- pandas library (pip install pandas)
- sqlalchemy (pip install sqlalchemy)
- Virtual environment in directory with Python Script

## Installation <a name="installation"></a>

Clone the repository to your local machine:

``` 
git clone https://github.com/Arshavin023/stored_procedures_and_parallel_execution.git
```

Install the required Python packages:

```
pip install -r requirements.txt
```


## Execution Flow
- Create/Update stored procedures on PostgreSQL database
- Navigate to directory containing Python Script
- Activate virtual environment
```
source virtual_env/bin/activate
```
- Run Python script
```
nohup python generate_radet_v2.py &
```