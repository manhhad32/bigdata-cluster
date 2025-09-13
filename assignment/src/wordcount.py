from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Khởi tạo SparkSession
    spark = SparkSession.builder.appName("SpecificWordCount").getOrCreate()
    sc = spark.sparkContext

    # --- BƯỚC 1: Đọc danh sách các từ cần đếm và broadcast nó ---
    target_words_path = "hdfs://namenode:9000/data/input/input.txt"
    
    # Đọc file, tách thành các từ, và thu thập danh sách về Driver
    target_words_list = spark.read.text(target_words_path).rdd \
                             .flatMap(lambda line: line.value.split(' ')) \
                             .collect()
    
    # Tạo một broadcast variable từ danh sách.
    # Biến này sẽ được gửi hiệu quả đến tất cả các worker.
    broadcast_target_words = sc.broadcast(set(target_words_list))

    # --- BƯỚC 2: Đọc file văn bản chính ---
    main_text_path = "hdfs://namenode:9000/data/data.txt"
    lines = spark.read.text(main_text_path).rdd.map(lambda r: r[0])

    # --- BƯỚC 3: Tách từ, lọc theo danh sách broadcast, và đếm ---
    counts = lines.flatMap(lambda line: line.split(' ')) \
                  .filter(lambda word: word in broadcast_target_words.value) \
                  .map(lambda word: (word, 1)) \
                  .reduceByKey(lambda a, b: a + b)

    # --- BƯỚC 4: In một phần kết quả và Ghi ra HDFS ---
    
    # In ra màn hình 20 kết quả đầu tiên để xem trước.
    # .take(n) là một cách an toàn để xem một phần dữ liệu mà không làm quá tải Driver.
    print("--- XEM TRUOC 20 KET QUA DAU TIEN ---")
    results_sample = counts.take(20)
    for (word, count) in results_sample:
        print(f"{word}: {count}")
    print("---------------------------------------")

    # Ghi kết quả ra HDFS theo cách song song (chuẩn)
    output_path = "hdfs://namenode:9000/data/wordcount_output"
    
    # Spark sẽ tự động ghi kết quả ra một thư mục 
    # Đây là cách làm hiệu quả nhất cho dữ liệu lớn.chứa nhiều file part-xxxxx
    counts.saveAsTextFile(output_path)
    
    print(f"Hoan thanh dem tu cu the! Kiem tra ket qua tai thu muc {output_path} tren HDFS.")

    spark.stop()