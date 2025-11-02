## Kiến trúc hệ thống (Bản rút gọn)
Hadoop HDFS: 1 Namenode, 1 Datanode
Apache Spark: 1 Master, 1 Worker
Apache Kafka: 3 Broker (chế độ KRaft), 1 Kafka UI
## Yêu cầu
Docker và Docker Compose.
Python 3.x
Thư viện kafka-python (chạy trên máy thật của bạn)
```sh
# Dùng pip
pip install kafka-python
```
```sh
# Hoặc nếu bạn dùng Conda
conda install -c conda-forge kafka-python (nếu là macOS trước đó đã cài python bằng conda)
```

## Hướng dẫn cài đặt và Vận hành
Bước 1: Khởi động hệ thống
Chạy lệnh này từ thư mục chứa file docker-compose.yml
```sh
docker compose up -d
```
Bước 2: Cấu hình môi trường
1. Tạo thư mục Checkpoint trên HDFS: Spark cần nơi này để lưu trạng thái.
```sh
docker compose exec namenode hdfs dfs -mkdir -p /spark-checkpoints
```

2. Tạo Topic Kafka: Chúng ta tạo topic web_logs với 1 partition và 1 bản sao
```sh
docker compose exec kafka1 kafka-topics \
  --create \
  --topic web_logs \
  --bootstrap-server kafka1:9092,kafka2:9092,kafka3:9092 \
  --partitions 3 \
  --replication-factor 3 \
  --config min.insync.replicas=2
```
Bước 3: Chạy Consumer (Spark Job)
1. Copy file script vào Spark Master: Đảm bảo file process_logs.py
```sh
docker cp process_logs.py spark-master:/spark/
```
2. Submit Spark Job: Chạy lệnh spark-submit bên trong container spark-master.
```sh
docker compose exec spark-master /spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
  /spark/process_logs.py
```

Bước 4: Chạy Producer (Gửi dữ liệu)
```sh
python producer.py
```