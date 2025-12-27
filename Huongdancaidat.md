# Big Data Cluster

## Phiên bản cài đặt

- **Hadoop:** 3.2.1  
- **Spark:** 3.3.0

## Truy cập giao diện web

- Spark: [http://localhost:8080](http://localhost:8080)
- Hadoop : [http://localhost:9870](http://localhost:9870)
- Nifi: [https://localhost:8443/nifi](https://localhost:8443/nifi)

---

## Kiến trúc hệ thống triển khai

- **1 Namenode:** "Master" của Hadoop, quản lý hệ thống file HDFS và tài nguyên YARN.
- **3 Datanode:** "Worker" của Hadoop, chịu trách nhiệm lưu trữ dữ liệu thực tế.
- **1 Spark Master:** "Master" của Spark, quản lý và điều phối các tác vụ.
- **3 Spark Worker:** "Worker" của Spark, chịu trách nhiệm thực thi các tác vụ tính toán.

> **Lưu ý:** Để đơn giản hóa, Spark Master và Hadoop Namenode sẽ chạy trên cùng một container `master`.

---

## Khởi động và kiểm tra cluster

**Khởi động cluster:**
```sh
docker compose up -d
```

**Kiểm tra trạng thái các container:**
```sh
docker ps
```

---

## Làm việc với HDFS

Bạn có thể thao tác với HDFS từ bất kỳ container nào.

**Tạo thư mục trên HDFS:**
```sh
docker exec namenode hdfs dfs -mkdir -p /user/test
```

**Chép file từ local vào HDFS:**
```sh
# Tạo một file mẫu
echo "Hello Big Data" > my_file.txt

# Chép file vào container namenode
docker cp my_file.txt namenode:/

# Từ bên trong container, chép file vào HDFS
docker exec namenode hdfs dfs -put /my_file.txt /user/test
```

---

## Dọn dẹp hệ thống

**Tắt và xóa cluster, bao gồm cả volumes dữ liệu HDFS:**
```sh
docker compose down -v
```

---

## Các lệnh hữu ích khác

- **Khởi động lại cluster (khi container đang dừng):**
  ```sh
  docker compose start
  ```
- **Tạm dừng cluster (không xóa):**
  ```sh
  docker compose stop
  ```
- **Tắt và xóa hoàn toàn cluster:**
  ```sh
  docker compose down
  ```

---

## Gửi tác vụ Spark lên cluster

Ví dụ gửi bài toán tính số Pi lên cluster:

```sh
docker exec spark-master /spark/bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --executor-memory 1G \
  --total-executor-cores 3 \
  /spark/examples/jars/spark-examples_2.12-3.3.0.jar 100
```

---

## Mẹo: Tìm đường dẫn lệnh trong container

**Mở shell vào container:**
```sh
docker exec -it spark-master /bin/bash
```
**Tìm đường dẫn lệnh:**
```sh
which spark-submit
```

---

## Spark tương tác với HDFS

Bạn có thể thao tác với HDFS từ bất kỳ container nào.

**Tạo thư mục trên HDFS:**
```sh
docker exec namenode hdfs dfs -mkdir -p /user/test
```
- xoa :
hdfs dfs -rm -r -skipTrash /data

- Mở quyền ghi cho tất cả user vào thư mục /data trên HDFS
hdfs dfs -chmod -R 777 /data

**Chép file từ local vào HDFS:**
```sh
echo "Hello Big Data" > my_file.txt
docker cp my_file.txt namenode:/
docker exec namenode hdfs dfs -put /my_file.txt
```

***Sau Khi start NiFi nếu không login được bằng user/pass đã setup thì tìm giá trị default bằng cách:
```sh
docker compose logs nifi | grep "Generated"
```
***Kiểm tra service hive-metastore nếu ko start được thì chạy cmd:
```sh
docker exec -it hive-metastore /opt/hive/bin/schematool -dbType postgres -initSchema
```
và restart lại hive-metastore:
```sh
docker restart hive-metastore
```
## Reset Mật khẩu của nifi:

- Truy cập vào shell của container
  ```sh
  docker exec -it nifi bash
  ```
- đổi mật khẩu: (set username là admin, mật khẩu là AdminPassword123)
  ```sh
  ./bin/nifi.sh set-single-user-credentials admin AdminPassword123
  ```
- Thoát khỏi container 
  ```sh
  exit
  ```
- Khởi động lại Nifi:
  ```sh
  docker restart nifi
  ```
## Hướng dẫn run code:  
- Q5:
  Để chạy file src/analysis_spark.py trên spark-master:
  - b1: Copy file code vào trong container spark-master
  Tạo thư mục src trên spark-master trước sau đó copy:
  ```sh
  docker cp analysis_spark.py spark-master:/src/spark/
  ```
  - b2: Truy cập vào container Spark Master
  ```sh
  docker exec -it spark-master /bin/bash
  ```
  - b3: Submit lệnh Spark
  ```sh
  spark/bin/spark-submit --master spark://spark-master:7077 src/analysis_spark.py
  ```

  -Q7:
  ```sh
  spark/bin/spark-submit --jars opt/spark_drivers/postgresql-42.7.7.jar opt/spark_apps/etl_spark_q7.py
  ```