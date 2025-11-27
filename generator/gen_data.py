import csv
import random
import time
import os
from datetime import datetime

# --- CẤU HÌNH ---
# Đây là đường dẫn trên máy HOST (máy thật của bạn)
# Thư mục này phải khớp với thư mục bạn đã mount vào container NiFi trong docker-compose
# Trong docker-compose trước đó: - ./data_source:/opt/nifi/nifi-current/data_source
LOCAL_SOURCE_DIR = "./data_source" 
SHOPS = 60

PRODUCTS = [
    (101, "Cà phê Mocha Đá", 39000),
    (102, "Espresso", 59000),
    (101, "Cà phê sữa", 39000), 
    (103, "Trà Đào Cam Sả", 45000),
    (104, "Bạc Xỉu", 35000)
]

def generate_file():
    # 1. Giả lập thông tin
    shop_id = random.randint(1, SHOPS)
    now = datetime.now()
    date_str = now.strftime('%Y%m%d')
    hour_str = now.strftime('%H')
    
    # Thêm timestamp để giả lập nhiều file trong thời gian ngắn mà không bị ghi đè
    filename = f"Shop-{shop_id}-{date_str}-{hour_str}_{int(time.time())}.csv"
    
    # Đường dẫn file nằm trên máy HOST
    filepath = os.path.join(LOCAL_SOURCE_DIR, filename)

    # 2. Sinh dữ liệu
    num_orders = random.randint(2, 10)
    data = []
    
    for _ in range(num_orders):
        order_prefix = now.strftime('%Y%m')
        order_id = int(f"{order_prefix}{random.randint(1000, 9999)}")
        
        prod = random.choice(PRODUCTS)
        p_id = prod[0]
        p_name = prod[1]
        price = prod[2]
        amount = random.randint(1, 5)
        discount = 0 

        data.append([order_id, p_id, p_name, amount, price, discount])

    # 3. Ghi file (Chỉ ghi ra ổ cứng, KHÔNG gọi lệnh HDFS ở đây)
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # Không ghi Header theo style của đề bài để tiện load vào Hive
            writer.writerows(data)
        print(f"-> [Shop {shop_id}] Đã gửi file: {filename} vào folder chờ ETL.")
    except Exception as e:
        print(f"Lỗi ghi file: {e}")

if __name__ == "__main__":
    # Tạo thư mục trên máy Host nếu chưa có
    if not os.path.exists(LOCAL_SOURCE_DIR):
        os.makedirs(LOCAL_SOURCE_DIR)
        print(f"Đã tạo thư mục: {LOCAL_SOURCE_DIR}")
    
    print(f"--- Bắt đầu giả lập dữ liệu ---")
    print(f"File sẽ được sinh ra tại: {os.path.abspath(LOCAL_SOURCE_DIR)}")
    print("NiFi cần được cấu hình để 'lắng nghe' thư mục này.")
    
    while True:
        generate_file()
        # Chỉnh thời gian delay tùy ý (ví dụ 5 giây sinh 1 file)
        time.sleep(5)