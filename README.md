# The assignment is divided into 5 parts as follows:

## PART 1: ENVIRONMENT PREPARATION

### Use Docker to deploy components in the Big Data processing architecture, including:

- Hadoop Cluster
  - 1 namenode (master), 2 datanodes (workers)
- Spark Cluster
  - 1 master, 2 workers.
- Database & ETL Tool
  - Database: Postgres
  - ETL Tool: NiFi
- Hive Cluster
  - Hive Metastore (hive-metastore, metastore)
  - Hive Server (hive-server, hiveserver2)

## PART 2: STREAMING & ETL

(Questions 2, 3)
- Question 2: Streaming
  - Task: Act as a system of stores generating sales data stored in the directory (`/home/hduser/data`), continuously pushing sales data to `/home/hduser/realtime-data` locally.
  - Code files: `gen_data.py`, `simulate_streaming.py`
  - Command:
  ```sh
  python src/gen_data.py
  python src/simulate_streaming.py
  ```

- Question 3: ETL - Using NiFi
  - Use NiFi to create a data flow that transfers all data files from `/home/hduser/realtime-data` to `/data` on HDFS (namenode - hadoop).

## PART 3: HIVE WAREHOUSE

(Question 4)
- Use DBeaver to connect to Hive (hive-server)
  - Create tables using the commands in the file: `src/Create_table_on_hive.sql`
  - Verify results using:
  ```sh
  SELECT * FROM sales_db.orders LIMIT 20;
  ```

## PART 4: DATA ANALYSIS (Questions 5, 6, 7)

(Question 5)
- Use Spark (pySpark) to write analysis calculation code as required in question 5.
- Code file: `src/analysis_spark.py`
- Steps to run this code on `spark-master`:
  - Step 1: Copy the code file into the `spark-master` container.
  Create the `src` directory on `spark-master` first, then copy:
  ```sh
  docker cp analysis_spark.py spark-master:/src
  ```
  - Step 2: Access the Spark Master container
  ```sh
  docker exec -it spark-master /bin/bash
  ```
  - Step 3: Submit the Spark command
  ```sh
  spark/bin/spark-submit --master spark://spark-master:7077 src/analysis_spark.py
  ```

  - Questions 6, 7

## PART 5: REPORTING (Question 8)

- Use Power BI to create reports as required.
  
## Contact

email: ha.nguyen.fzx@gmail.com | manhhad32@gmail.com