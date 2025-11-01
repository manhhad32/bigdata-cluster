import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, regexp_extract

if __name__ == "__main__":
    
    # 1. Khởi tạo SparkSession
    # Cần chỉ định Spark Master và gói (package) Kafka
    spark = (
        SparkSession.builder.appName("KafkaLogAnalysis")
        .master("spark://spark-master:7077")
        # QUAN TRỌNG: Phải thêm gói này để Spark có thể nói chuyện với Kafka
        # Phiên bản 3.3.0 phải khớp với phiên bản Spark trong docker-compose.yml
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
        .getOrCreate()
    )
    
    # Giảm log rác của Spark
    spark.sparkContext.setLogLevel("WARN")

    print("Spark Session đã khởi tạo, đang kết nối Kafka...")

    # 2. Đọc Stream từ Kafka
    # Spark sẽ kết nối đến các broker trong mạng nội bộ docker
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092")
        .option("subscribe", "web_logs")
        .option("startingOffsets", "latest") # Bắt đầu đọc từ dữ liệu mới nhất
        .load()
    )

    # 3. Xử lý dữ liệu
    # Dữ liệu từ Kafka có dạng (key, value, ...), 'value' là nội dung log
    # Chuyển 'value' từ dạng binary sang STRING
    log_df = kafka_df.selectExpr("CAST(value AS STRING) as log_line")

    # Phân tách (parse) dòng log để lấy status code
    # Sử dụng Regular Expression để trích xuất 3 chữ số ở cuối dòng
    # Định dạng: [IP] [TS] [METHOD] [URL] [STATUS]
    # Regex: '(\d{3})$' nghĩa là "tìm 3 chữ số (\d{3}) ở cuối dòng ($)"
    
    log_pattern = r'^\S+ \S+ \S+ \S+ (\d{3})$'
    
    status_df = log_df.select(
        regexp_extract(col("log_line"), log_pattern, 1).alias("status_code")
    )

    # Lọc ra những dòng không khớp (status_code = rỗng)
    valid_status_df = status_df.filter(col("status_code") != "")

    # 4. Thực hiện Aggregation (Đếm)
    # Đếm số lượng theo từng status_code
    counts_df = valid_status_df.groupBy("status_code").count()

    # 5. Viết Stream ra Console
    # outputMode("complete"): In ra toàn bộ bảng đếm mỗi khi có cập nhật
    query = (
        counts_df.writeStream.outputMode("complete")
        .format("console")
        .option("truncate", "false") # Hiển thị đầy đủ nội dung cột
        .option("checkpointLocation", "hdfs://namenode:9000/spark-checkpoints/web_logs")
        .start()
    )

    print("Đang chờ dữ liệu stream...")
    # Chờ cho đến khi stream bị ngắt
    query.awaitTermination()