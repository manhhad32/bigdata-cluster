# Bài tập chia làm 5 phần như sau:

## Phần 1: Chuẩn bị môi trường

### Sử dụng Docker để triển khai các thành phần trong kiến trúc xử lý bigdata gồm:

- Cụm Hadoop
  - 1 namenode(master), 2 datanode(worker)
- Cụm Spark
  - 1 master, 2 worker.
- Database & Tool ETL
  - Database: Postgres
  - Tool ETL: Nifi
- Cụm Hive
  - Hive Meta store (hive-metastore, metatstore)
  - Hive server(hive-server, hiveserve2)

## Phần 2: Giả lập Stream & ETL (câu 2, 3)

- Câu 2: Giả lập Streaming
  - Nhiệm vụ: Đóng vai trò là hệ thống các cửa hàng tạo dữ liệu bán hàng lưu ở thư muc (/home/hduser/data), liên tục đẩy dữ liệu bán hàng vào /home/hduser/realtime-data ở local.
  - file code: gen_data.py, simulate_streaming.py, 
  - cmd:
  ```sh
  python scr/gen_data.py
  python src/simulate_streaming.py
  ```

- Câu 3: ETL - sử dụng Nifi
  - Sư dụng Nifi để tạo flow data đưa toàn bộ file dữ liệu từ /home/hduser/realtime-data lên /data của HDFS (namenode - hadoop)
  
## Phần 3: HIVE WAREHOUSE

(câu 4)
- Sử dụng BDeaver để connect tới Hive (hive-server)
  - Tạo bảng bằng câu lệnh trong file: src/Create_table_on_hive.sql
  - Kiểm tra kết quả bằng: 
  ```sh
  SELECT * FROM sales_db.orders LIMIT 20;
  ```

## Phần 4: PHÂN TÍCH SPARK & BÁO CÁO (Câu 5, 6, 7)

(câu 5)
- Sử dụng Spack (pySpark) để viết code tính toán phân tích theo yêu cầu - câu 5.
- file code: src/analysis_spark.py
- các bước run code này trên spark-master:
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

  -câu 6,7

## Phần 5: BÁO CÁO (Câu 8)

- Sử dụng Power BI tạo các báo cáo theo yêu cầu
