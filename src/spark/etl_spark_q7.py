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

def main():
    # 1. Khởi tạo Spark
    spark = SparkSession.builder \
        .appName("Final_ETL_Updated_Logic") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # 2. Định nghĩa Schema (Cập nhật thêm Discount)
    # File CSV có 6 cột: OrderID, ProductID, ProductName, Amount, Price, Discount
    schema = StructType([
        StructField("OrderID", LongType()),
        StructField("ProductID", IntegerType()),
        StructField("ProductName", StringType()),
        StructField("Amount", IntegerType()),
        StructField("Price", LongType()), # Trong code mẫu bạn dùng int/long cho giá
        StructField("Discount", LongType()) 
    ])

    print("--- [E]xtract: Đọc dữ liệu từ HDFS ---")
    # Đọc csv và thêm cột filename để lấy ShopID
    raw_df = spark.read.schema(schema) \
        .option("header", "false") \
        .csv("hdfs://namenode:9000/data/*.csv") \
        .withColumn("FilePath", input_file_name())

    # 3. [T]ransform: Áp dụng Logic chuẩn từ analysis_spark.py
    print("--- [T]ransform: Làm sạch và Tính toán ---")
    
    df_clean = raw_df \
        .withColumn("ShopID", regexp_extract("FilePath", r"Shop-(\d+)", 1).cast("int")) \
        .withColumn("YearMonth", substring(col("OrderID").cast("string"), 1, 6)) \
        .withColumn("Year", substring(col("OrderID").cast("string"), 1, 4)) \
        .withColumn("Revenue", (col("Price") * col("Amount")) - col("Discount"))

    # Cache lại để dùng cho 4 câu lệnh sau
    df_clean.cache()

    # --- 5a. Top Sản phẩm bán chạy nhất (Toàn thời gian) ---
    # Logic: Group by Product -> Sum Amount
    print("Processing 5a...")
    res_5a = df_clean.groupBy("ProductID", "ProductName") \
        .agg(sum("Amount").alias("TotalQuantity")) \
        .orderBy(desc("TotalQuantity"))
    
    # --- 5b. Top Sản phẩm bán chạy (Theo Tháng) ---
    # Logic: Group by Month, Product. 
    # Lưu ý: Ta lưu TOÀN BỘ các tháng vào DB, khi cần xem tháng 202303 chỉ cần SELECT ... WHERE YearMonth='202303'
    print("Processing 5b...")
    res_5b = df_clean.groupBy("YearMonth", "ProductID", "ProductName") \
        .agg(sum("Amount").alias("TotalQuantity")) \
        .orderBy("YearMonth", desc("TotalQuantity"))

    # --- 5c. Doanh thu sản phẩm (Theo Năm) ---
    # Logic: Group by Year, Product -> Sum Revenue (đã trừ discount)
    print("Processing 5c...")
    res_5c = df_clean.groupBy("Year", "ProductID", "ProductName") \
        .agg(sum("Revenue").alias("TotalRevenue")) \
        .orderBy("Year", desc("TotalRevenue"))

    # --- 5d. Doanh thu Shop (Theo Tháng) ---
    # Logic: Group by Month, Shop -> Sum Revenue
    print("Processing 5d...")
    res_5d = df_clean.groupBy("YearMonth", "ShopID") \
        .agg(sum("Revenue").alias("ShopRevenue")) \
        .orderBy("YearMonth", "ShopID")

    # 4. [L]oad: Ghi vào PostgreSQL
    print("--- [L]oad: Ghi kết quả vào Database ---")

    # Ghi đè (Overwrite) để đảm bảo dữ liệu mới nhất
    res_5a.write.mode("overwrite").jdbc(DB_URL, "report_5a_top_products_alltime", properties=DB_PROPS)
    res_5b.write.mode("overwrite").jdbc(DB_URL, "report_5b_top_products_monthly", properties=DB_PROPS)
    res_5c.write.mode("overwrite").jdbc(DB_URL, "report_5c_revenue_products_yearly", properties=DB_PROPS)
    res_5d.write.mode("overwrite").jdbc(DB_URL, "report_5d_revenue_shop_monthly", properties=DB_PROPS)

    print("--- [SUCCESS] Đã ETL thành công! ---")
    spark.stop()

if __name__ == "__main__":
    main()