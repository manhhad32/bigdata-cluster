# Assignment practice spark on Hadoop & Spark architecture
  - prepare data text in data.txt
  - prepare input text in input.txt
  - prepare code python wordcount.py
## Step
  - ### 1. Create folder HDFS
    - use docker from local 
    ```sh
    docker exec namenode hdfs dfs -mkdir -p /data/input
    ```
    - From terminal of namenode:
    ``` sh
    docker exec -it namenode /bin/bash
    hdfs dfs -mkdir -p /data/input
    ```
    - check again:
      ```sh
      hdfs dfs -ls /data
      ```
  - ### 1. copy data into namenode (master hadoop)
    - from local - using docker cmd
    ```sh
    docker exec namenode hdfs dfs -put data.txt /data/
    ```
    or from namenode image
      ```sh
      hdfs dfs -put data.txt /data/
      hdfs dfs -put /input/input.txt /data/input/
      ```
  - ### 3. copy src wordcount.py into master-spark
    - using docker from local
    ```sh
    docker cp wordcount.py spark-master:/
    ```
    - run src:
    - from local
    ```sh
    # job Spark start
    docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /wordcount.py
    ```
    - from spark-master
    ```sh
    docker exec -it spark-master /bin/bash
    spark/bin/spark-submit --master spark://spark-master:7077 wordcount.py
    ```

  