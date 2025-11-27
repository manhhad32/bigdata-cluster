#Q5
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, desc, input_file_name, regexp_extract, substring, expr

# --- CẤU HÌNH ---
HDFS_PATH = "hdfs://namenode:9000/data/*.csv"

def main():
    # 1. Khởi tạo Spark Session
    spark = SparkSession.builder \
        .appName("RetailBigDataAnalysis") \
        .getOrCreate()

    # Tắt log rác để dễ nhìn kết quả
    spark.sparkContext.setLogLevel("WARN")

    print(">>> Đang đọc dữ liệu từ HDFS...")

    # 2. Đọc dữ liệu CSV (Schema thủ công vì file không có header)
    df = spark.read.csv(HDFS_PATH, header=False, inferSchema=True)
    
    # Đặt tên cột
    df = df.toDF("OrderID", "ProductID", "ProductName", "Amount", "Price", "Discount")

    # 3. Xử lý dữ liệu (ETL)
    # - Trích xuất ShopID từ tên file (input_file_name)
    # - Trích xuất YearMonth từ OrderID (6 ký tự đầu)
    # - Tính cột Revenue (Doanh thu)
    df_clean = df.withColumn("FilePath", input_file_name()) \
                 .withColumn("ShopID", regexp_extract("FilePath", r"Shop-(\d+)", 1).cast("int")) \
                 .withColumn("YearMonth", substring(col("OrderID").cast("string"), 0, 6)) \
                 .withColumn("Revenue", (col("Price") * col("Amount")) - col("Discount"))

    # Cache lại vì chúng ta sẽ dùng dataframe này cho cả 4 câu hỏi
    df_clean.cache()
    
    print(f">>> Tổng số dòng dữ liệu: {df_clean.count()}")
    
    # --- BÀI TOÁN 5a: Top K sản phẩm bán chạy nhất toàn hệ thống ---
    K_a = 5
    print(f"\n=== 5a. Top {K_a} sản phẩm bán chạy nhất (Toàn hệ thống) ===")
    df_clean.groupBy("ProductID", "ProductName") \
            .agg(_sum("Amount").alias("TotalQuantity")) \
            .orderBy(desc("TotalQuantity")) \
            .show(K_a, truncate=False)

    # --- BÀI TOÁN 5b: Top K sản phẩm tại thời điểm D (Tháng-Năm) ---
    K_b = 3
    D_b = "202303" # Ví dụ: Tháng 3 năm 2023
    print(f"\n=== 5b. Top {K_b} sản phẩm bán chạy nhất tháng {D_b} ===")
    df_clean.filter(col("YearMonth") == D_b) \
            .groupBy("ProductID", "ProductName") \
            .agg(_sum("Amount").alias("TotalQuantity")) \
            .orderBy(desc("TotalQuantity")) \
            .show(K_b, truncate=False)

    # --- BÀI TOÁN 5c: Doanh thu trên mỗi sản phẩm trong năm ---
    Year_c = "2023"
    print(f"\n=== 5c. Doanh thu từng sản phẩm trong năm {Year_c} ===")
    df_clean.filter(col("YearMonth").startswith(Year_c)) \
            .groupBy("ProductID", "ProductName") \
            .agg(_sum("Revenue").alias("TotalRevenue")) \
            .orderBy(desc("TotalRevenue")) \
            .show(10, truncate=False) # Show 10 cái tiêu biểu

    # --- BÀI TOÁN 5d: Doanh thu mỗi Shop tại thời điểm D ---
    D_d = "202303"
    print(f"\n=== 5d. Doanh thu từng Shop trong tháng {D_d} ===")
    df_clean.filter(col("YearMonth") == D_d) \
            .groupBy("ShopID") \
            .agg(_sum("Revenue").alias("ShopRevenue")) \
            .orderBy(desc("ShopRevenue")) \
            .show(10)

    spark.stop()

if __name__ == "__main__":
    main()