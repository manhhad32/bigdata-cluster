from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType, DoubleType
from pyspark.sql.functions import input_file_name

def main():
    # 1. Khởi tạo Spark Session
    spark = SparkSession.builder \
        .appName("Question6_Verify_SparkSQL") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # 2. Định nghĩa Schema (Cấu trúc dữ liệu)
    # Cấu trúc: OrderID,ProductID,ProductName,Amount,Price,Discount (nếu có)
    schema = StructType([
        StructField("OrderID", LongType()),
        StructField("ProductID", IntegerType()),
        StructField("ProductName", StringType()),
        StructField("Amount", IntegerType()),
        StructField("Price", DoubleType()),
        StructField("Discount", DoubleType())
    ])

    # 3. Đọc dữ liệu từ HDFS
    # Spark sẽ tự động đọc TẤT CẢ các file .csv trong thư mục /data
    print("--- Đang đọc dữ liệu từ HDFS /data ---")
    df = spark.read \
        .schema(schema) \
        .option("header", "false") \
        .csv("hdfs://namenode:9000/data/*.csv") \
        .withColumn("FilePath", input_file_name())

    # 4. Tạo bảng tạm (Temporary View) để chạy SQL
    df.createOrReplaceTempView("orders")

    # --- KIỂM TRA YÊU CẦU 5a: Top K sản phẩm bán chạy nhất ---
    # Logic: Group theo tên sản phẩm -> Tính tổng số lượng (Amount) -> Sắp xếp giảm dần
    K = 5
    print(f"\n=== [SQL 5a] top {K} sản phẩm bán chạy nhất (theo Số lượng - toàn hệ thống) ===")
    spark.sql(f"""
        SELECT 
            ProductName, 
            format_number(SUM(Amount), 0) as TotalQuantity
        FROM orders
        GROUP BY ProductName
        ORDER BY SUM(Amount) DESC
        LIMIT {K}
    """).show(truncate=False)

    # --- KIỂM TRA YÊU CẦU 5c: Doanh thu trên mỗi sản phẩm ---
    # Logic: Group theo tên -> Tính tổng (Amount * Price) -> Sắp xếp giảm dần
    print("\n=== [SQL 5c] Doanh thu từng sản phẩm (Năm 2023) ===")
    df_5c = spark.sql("""
        SELECT 
            ProductName, 
            format_number(CAST(SUM((Amount * Price) - Discount) AS DECIMAL(18, 2)), 0) as TotalRevenue
        FROM orders
        WHERE substring(CAST(OrderID AS STRING), 1, 4) = '2023'
        GROUP BY ProductName
        ORDER BY SUM((Amount * Price) - Discount) DESC
    """)
    df_5c.show(df_5c.count(), truncate=False)

    # --- KIỂM TRA YÊU CẦU 5d: Doanh thu từng Shop ---
    #print("\n=== [SQL 5d] Doanh thu từng Shop (Tháng 03/2023) ===")
    #df_5d = spark.sql("""
    #    SELECT 
    #        CAST(regexp_extract(FilePath, 'Shop-(\\\\d+)', 1) AS INT) as ShopID,
    #        format_number(CAST(SUM((Amount * Price) - Discount) AS DECIMAL(18, 2)), 0) as ShopRevenue
    #    FROM orders
    #    WHERE substring(CAST(OrderID AS STRING), 1, 6) = '202303'
    #    GROUP BY ShopID
    #    ORDER BY SUM((Amount * Price) - Discount) DESC
    #""")
    #df_5d.show(df_5d.count(), truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()