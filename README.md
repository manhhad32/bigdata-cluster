# Bài tập chia làm 5 phần như sau:

## Phần 1: Chuẩn bị môi trường

### Sử dụng Docker để triên khai các thành phần trong kiến trúc xử lý bigdata gồm:

- Cụm Hadoop
  - 1 namenode(master), 2 datanode(worker)
- Cụm Spark
  - 1 master, 2 worker.
- Database & Tool ETL
  - Database: Postgres
  - Tool ETL: Nifi (cân nhắc hi sinh để dành resource cho kafka)
- Cụm Hive
  - Hive Meta store (hive-metastore, metatstore)
  - Hive server(hive-server, hiveserve2)
- Cụm Kafka 
  - Để tiết kiệm tài nguyên nên chỉ sử dụng 1 broker.

### Tạo dữ liệu giả lập trên HDFS

- run code : setup_data.py để sinh ra 388.800 files csv dữ liệu mẫu, trên thư mục: /home/hduser/data của Hadoop.
- run lần lượt các câu lệnh sau: 
  ```sh
  docker cp setup_data.py namenode:/tmp/
  ```
  ```sh
  docker exec namenode python3 /tmp/setup_data.py
  ```

## Phần 2: Giả lập Stream & ETL (câu 2, 3)

- để chuẩn bị topic trên kafka cho câu2,3 tạo topic:
  ```sh
  docker exec -it kafka1 kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic store_data \
    --partitions 3 \
    --replication-factor 3
  ```
  kiểm tra lại topic tạo thành công chưa:
  ```sh
  docker exec -it kafka1 kafka-topics --describe --topic store_data --bootstrap-server localhost:9092
  ```

- Câu 2: Giả lập Streaming (Producer)
  - Nhiệm vụ: Đóng vai trò là hệ thống các cửa hàng (/home/hduser/data), liên tục đẩy dữ liệu bán hàng vào /home/hduser/realtime-data trên HDFS.
  - file code: producer_streaming.py
- Câu 3: ETL Worker (Consumer)
  - Nhiệm vụ: Đóng vai trò là quy trình Backend, liên tục quét vùng chờ, lấy dữ liệu về kho chính (Warehouse) để làm sạch và lưu trữ lâu dài.
  - file code: consumer_etl.py

- cmd:
  ```sh
  docker cp producer_streaming.py namenode:/tmp/
  docker cp consumer_etl.py namenode:/tmp/
  ```
- Chạy Consumer (Cửa sổ Terminal 1)
  - Chạy script ETL trước để chờ dữ liệu.
  ```sh
  docker exec -it namenode python3 /tmp/consumer_etl.py
  ```
  - Chạy Producer (Cửa sổ Terminal 2):
  ```sh
  docker exec -it namenode python3 /tmp/producer_streaming.py
  ```