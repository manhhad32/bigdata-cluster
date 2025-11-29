import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, input_file_name, regexp_extract, substring

# --- CẤU HÌNH ---
HDFS_FOLDER = "/data"            
NAMENODE = "hdfs://namenode:9000" 
BATCH_SIZE = 500

# --- THAM SỐ YÊU CẦU ---
TARGET_MONTH = "202303" 
TARGET_YEAR = "2023"    
K_A = 5                 
K_B = 3                 

# --- BIẾN TÍCH LŨY ---
acc_5a = {} 
acc_5b = {}  
acc_5c = {}  
acc_5d = {}  

def get_hdfs_files_via_jvm_optimized(spark, hdfs_folder):
    """
    Phiên bản tối ưu: Sử dụng globStatus và in tiến độ
    """
    print(f"⏳ Đang kết nối tới NameNode để lấy danh sách file trong {hdfs_folder}...")
    start_time = time.time()
    
    try:
        sc = spark.sparkContext
        URI = sc._gateway.jvm.java.net.URI
        Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
        FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
        conf = sc._jsc.hadoopConfiguration()
        fs = FileSystem.get(URI(NAMENODE), conf)
        
        # TỐI ƯU 1: Dùng globStatus để lọc .csv ngay từ phía Java (Server side)
        # Thay vì lôi hết về rồi mới lọc bằng Python
        search_path = Path(hdfs_folder + "/*.csv")
        print(f"   -> Đang lấy file pattern: {hdfs_folder}/*.csv")
        
        # Bước này có thể mất 1-2 phút nếu NameNode bận
        file_statuses = fs.globStatus(search_path)
        
        if file_statuses is None:
            return []
            
        print(f"   -> Đã nhận phản hồi từ NameNode. Đang chuyển đổi dữ liệu...")
        
        files = []
        count = 0
        total_found = len(file_statuses)
        
        # TỐI ƯU 2: In tiến độ để người dùng không tưởng máy bị treo
        for status in file_statuses:
            # Lấy đường dẫn
            p = status.getPath().toString()
            files.append(p)
            
            count += 1
            if count % 10000 == 0:
                print(f"      ... Đã tải được {count}/{total_found} file ...")
                
        duration = time.time() - start_time
        print(f"✅ Hoàn tất lấy danh sách {len(files)} file trong {duration:.2f} giây.")
        return files

    except Exception as e:
        print(f"❌ Lỗi đọc HDFS: {e}")
        return []

def process_batch_logic(spark, batch_files):
    df = spark.read.csv(batch_files, header=False, inferSchema=True)
    df = df.toDF("OrderID", "ProductID", "ProductName", "Amount", "Price", "Discount")

    df_clean = df.withColumn("FilePath", input_file_name()) \
                 .withColumn("ShopID", regexp_extract("FilePath", r"Shop-(\d+)", 1).cast("int")) \
                 .withColumn("YearMonth", substring(col("OrderID").cast("string"), 0, 6)) \
                 .withColumn("Revenue", (col("Price") * col("Amount")) - col("Discount"))

    batch_agg = df_clean.groupBy("ProductID", "ProductName", "ShopID", "YearMonth") \
                        .agg(_sum("Amount").alias("Qty"), 
                             _sum("Revenue").alias("Rev"))
    
    return batch_agg.collect()

def update_global_results(batch_rows):
    for row in batch_rows:
        key_prod = (row['ProductID'], row['ProductName'])
        shop_id = row['ShopID']
        ym = row['YearMonth']
        qty = row['Qty']
        rev = row['Rev']

        acc_5a[key_prod] = acc_5a.get(key_prod, 0) + qty

        if ym == TARGET_MONTH:
            acc_5b[key_prod] = acc_5b.get(key_prod, 0) + qty
            acc_5d[shop_id] = acc_5d.get(shop_id, 0.0) + rev

        if ym.startswith(TARGET_YEAR):
            acc_5c[key_prod] = acc_5c.get(key_prod, 0.0) + rev

def print_result_table(title, data_dict, top_k, val_col_name, is_shop=False):
    print(f"\n{title}")
    print(f"{'ID':<15} | {'Name/Info':<30} | {val_col_name}")
    print("-" * 65)
    sorted_data = sorted(data_dict.items(), key=lambda x: x[1], reverse=True)[:top_k]
    for key, val in sorted_data:
        if is_shop:
            print(f"{str(key):<15} | {'Shop ID ' + str(key):<30} | {val:,.2f}")
        else:
            p_id, p_name = key
            print(f"{str(p_id):<15} | {p_name:<30} | {val:,.2f}")

def main():
    spark = SparkSession.builder \
        .appName("Final_Batch_Processing_Optimized") \
        .config("spark.driver.memory", "300m") \
        .config("spark.executor.memory", "1000m") \
        .config("spark.executor.memoryOverhead", "400m") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.ui.enabled", "false") \
        .config("spark.network.timeout", "800s") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # GỌI HÀM MỚI TỐI ƯU HƠN
    all_files = get_hdfs_files_via_jvm_optimized(spark, HDFS_FOLDER)
    
    total = len(all_files)
    if total == 0:
        print("❌ Không tìm thấy file hoặc quá trình đọc file bị lỗi.")
        spark.stop()
        return

    print(f"✅ Bắt đầu xử lý {total} file theo batch (Mỗi lần {BATCH_SIZE} file)...")

    for i in range(0, total, BATCH_SIZE):
        batch_files = all_files[i : i + BATCH_SIZE]
        batch_id = (i // BATCH_SIZE) + 1
        print(f"   -> Batch {batch_id}: Đang xử lý {len(batch_files)} file...")
        
        try:
            rows = process_batch_logic(spark, batch_files)
            update_global_results(rows)
            spark.catalog.clearCache()
        except Exception as e:
            print(f"   ⚠️ Lỗi batch {batch_id}: {e}")

    print("\n" + "="*30 + " KẾT QUẢ PHÂN TÍCH " + "="*30)
    
    print_result_table(f"=== 5a. Top {K_A} sản phẩm bán chạy nhất (Toàn hệ thống) ===", 
                       acc_5a, K_A, "Total Quantity")

    print_result_table(f"=== 5b. Top {K_B} sản phẩm bán chạy nhất tháng {TARGET_MONTH} ===", 
                       acc_5b, K_B, "Total Quantity")

    print_result_table(f"=== 5c. Top 10 Doanh thu từng sản phẩm trong năm {TARGET_YEAR} ===", 
                       acc_5c, 10, "Total Revenue")

    print_result_table(f"=== 5d. Top 10 Doanh thu từng Shop trong tháng {TARGET_MONTH} ===", 
                       acc_5d, 10, "Shop Revenue", is_shop=True)

    spark.stop()

if __name__ == "__main__":
    main()