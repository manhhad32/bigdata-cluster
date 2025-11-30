from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# --- CẤU HÌNH DATABASE ---
DB_URL = "jdbc:postgresql://postgres-db:5432/etl_db"
DB_PROPS = {
    "user": "etl_user",
    "password": "etlUser@123",
    "driver": "org.postgresql.Driver"
}

# --- THAM SỐ YÊU CẦU ---
TARGET_MONTH = "202303"  # Tháng 3 năm 2023 (Dùng cho 5b, 5d)
TARGET_YEAR  = "2023"    # Năm 2023 (Dùng cho 5c)
K_B = 3                  # Top 3 sản phẩm bán chạy (5b)
K_C = 10                 # Top 10 sản phẩm doanh thu cao nhất (5c)
K_D = 10                 # Top 10 shop doanh thu cao nhất (5d)
REPORT_5A = "report_5a_top5_qty_2023"
REPORT_5B = "report_5b_top3_qty_mar2023"
REPORT_5C = "report_5c_top10_rev2023"
REPORT_5D = "report_5d_top10_shop_rev_mar2023"
APP_NAME = "Final_ETL_Question7_Final_v2"


def main():
    # 1. Khởi tạo Spark
    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # 2. Định nghĩa Schema
    schema = StructType([
        StructField("OrderID", LongType()),
        StructField("ProductID", IntegerType()),
        StructField("ProductName", StringType()),
        StructField("Amount", IntegerType()),
        StructField("Price", LongType()),
        StructField("Discount", LongType()) 
    ])

    print("--- [E]xtract: Đọc dữ liệu từ HDFS ---")
    raw_df = spark.read.schema(schema) \
        .option("header", "false") \
        .csv("hdfs://namenode:9000/data/*.csv") \
        .withColumn("FilePath", input_file_name())

    # 3. [T]ransform: Làm sạch và chuẩn hóa dữ liệu
    df_clean = raw_df \
        .withColumn("ShopID", regexp_extract("FilePath", r"Shop-(\d+)", 1).cast("int")) \
        .withColumn("YearMonth", substring(col("OrderID").cast("string"), 1, 6)) \
        .withColumn("Year", substring(col("OrderID").cast("string"), 1, 4)) \
        .withColumn("Revenue", (col("Price") * col("Amount")) - col("Discount"))

    df_clean.cache()

    # ====================================================
    # XỬ LÝ CÁC YÊU CẦU
    # ====================================================

    # --- 5a. Top 5 Sản phẩm bán chạy nhất (Toàn hệ thống - All Time) ---
    print("Processing 5a (Top 5 Quantity - All Time)...")
    res_5a = df_clean.groupBy("ProductID", "ProductName") \
        .agg(sum("Amount").alias("TotalQuantity")) \
        .orderBy(desc("TotalQuantity")) \
        .limit(5)
    
    # --- 5b. Top 3 Sản phẩm bán chạy nhất (Tháng 03/2023) ---
    print(f"Processing 5b (Top {K_B} Quantity in {TARGET_MONTH})...")
    res_5b = df_clean.filter(col("YearMonth") == TARGET_MONTH) \
        .groupBy("ProductID", "ProductName") \
        .agg(sum("Amount").alias("TotalQuantity")) \
        .orderBy(desc("TotalQuantity")) \
        .limit(K_B)

    # --- 5c. Top 10 Sản phẩm doanh thu cao nhất (Năm 2023) ---
    # Cập nhật: Thêm limit(10)
    print(f"Processing 5c (Top {K_C} Revenue in {TARGET_YEAR})...")
    res_5c = df_clean.filter(col("Year") == TARGET_YEAR) \
        .groupBy("ProductID", "ProductName") \
        .agg(sum("Revenue").alias("TotalRevenue")) \
        .orderBy(desc("TotalRevenue")) \
        .limit(K_C)

    # --- 5d. Top 10 Shop doanh thu lớn nhất (Tháng 03/2023) ---
    print(f"Processing 5d (Top {K_D} Shops in {TARGET_MONTH})...")
    res_5d = df_clean.filter(col("YearMonth") == TARGET_MONTH) \
        .groupBy("ShopID") \
        .agg(sum("Revenue").alias("ShopRevenue")) \
        .orderBy(desc("ShopRevenue")) \
        .limit(K_D)

    # 4. [L]oad: Ghi vào PostgreSQL
    print("--- [L]oad: Ghi kết quả vào Database ---")

    
    res_5a.write.mode("overwrite").jdbc(DB_URL, REPORT_5A, properties=DB_PROPS)
    res_5b.write.mode("overwrite").jdbc(DB_URL, REPORT_5B, properties=DB_PROPS)
    res_5c.write.mode("overwrite").jdbc(DB_URL, REPORT_5C, properties=DB_PROPS)
    res_5d.write.mode("overwrite").jdbc(DB_URL, REPORT_5D, properties=DB_PROPS)

    print("--- [SUCCESS] Đã ETL thành công! ---")
    spark.stop()

if __name__ == "__main__":
    main()