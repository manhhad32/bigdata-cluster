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
    ```
    ``` sh
    hdfs dfs -mkdir -p /data/input
    ```
    - check again:
      ```sh
      hdfs dfs -ls /data
      ```
  - ### 2. copy data into namenode (master hadoop)
    - from local - using docker cmd
    ```sh
    docker exec namenode hdfs dfs -put data.txt /data/
    ```
    or from namenode image
      ```sh
      hdfs dfs -put data.txt /data/
      ```
      ```sh
      hdfs dfs -put /input/input.txt /data/input/
      ```
  - ### 3. copy src wordcount.py into master-spark
    - using docker from local
    ```sh
    docker cp wordcount.py spark-master:/
    ```
    - run src:
    - from local
    #job Spark start
    ```sh
    docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /wordcount.py
    ```
    - from master-spark
    ```sh
    docker exec -it spark-master /bin/bash
    ```
    ```sh
    spark/bin/spark-submit --master spark://spark-master:7077 wordcount.py
    ```

  