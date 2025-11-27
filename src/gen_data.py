import csv
import random
import os
import time
from datetime import datetime, timedelta

# --- CẤU HÌNH ---
# Thư mục lưu trữ dữ liệu gốc (Kho dữ liệu)
# Tương ứng với /home/hduser/data trong đề bài
LOCAL_SOURCE_DIR = "/Users/nguyenmanhha/Desktop/data" 
SHOPS = 60
DAYS_TO_GENERATE = 360 # Giả lập 1 năm kinh doanh (360 ngày)
HOURS_PER_DAY = 18     # Cửa hàng mở cửa 18 tiếng/ngày (ví dụ 06h - 24h)

# Tổng số file dự kiến: 60 * 18 * 360 = 388.800 file

PRODUCTS = [
    (100, "Cà phê Mocha Đá", 39000),
    (102, "Espresso", 59000),
    (101, "Cà phê sữa", 39000), 
    (103, "Trà Đào Cam Sả", 45000),
    (104, "Bạc Xỉu", 35000),
    (105, "Trà Sen Vàng", 55000),
    (106, "Freeze Trà Xanh", 65000),
    (107, "Trà Sữa", 75000),
    (108, "Dừa Tươi", 85000),
    (109, "cafe đá", 35000),
    (110, "cafe Sữa Nóng size S", 45000)
]

def generate_batch_data():
    if not os.path.exists(LOCAL_SOURCE_DIR):
        os.makedirs(LOCAL_SOURCE_DIR)
        print(f"Đã tạo thư mục kho: {LOCAL_SOURCE_DIR}")

    start_date = datetime(2023, 1, 1) # Bắt đầu từ ngày 1/1/2023
    total_files = 0
    
    print(f"--- Bắt đầu sinh 388.800 file tại {LOCAL_SOURCE_DIR} ---")
    print("Vui lòng chờ, quá trình này có thể mất vài phút...")

    # Vòng lặp theo NGÀY
    for day in range(DAYS_TO_GENERATE):
        current_date = start_date + timedelta(days=day)
        date_str = current_date.strftime('%Y%m%d')
        
        # Vòng lặp theo GIỜ (Giả sử mở từ 6h sáng đến 23h đêm -> 18 khung giờ)
        for h in range(6, 6 + HOURS_PER_DAY):
            hour_str = f"{h:02d}" # Format thành '06', '07'...
            
            # Vòng lặp theo SHOP (60 shop gửi dữ liệu cùng lúc trong giờ đó)
            for shop_id in range(1, SHOPS + 1):
                # Tên file chuẩn: Shop-k-YYYYMMDD-hh.csv
                filename = f"Shop-{shop_id}-{date_str}-{hour_str}.csv"
                filepath = os.path.join(LOCAL_SOURCE_DIR, filename)
                
                # --- Sinh nội dung file ---
                total_items_in_file = random.randint(5, 1000) # Random số lượng đơn hàng
                data = []
                for _ in range(total_items_in_file):
                    order_prefix = date_str
                    order_id = int(f"{order_prefix}{random.randint(1000, 9999)}")
                    
                    prod = random.choice(PRODUCTS)
                    p_id, p_name, price = prod
                    
                    amount = random.randint(1, 10)
                    discount = int(price * amount * 0.1) if random.random() < 0.1 else 0
                    
                    data.append([order_id, p_id, p_name, amount, price, discount])

                # Ghi file
                try:
                    with open(filepath, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        # Bỏ header để tiện cho Hive load data
                        writer.writerows(data)
                    
                    total_files += 1
                except Exception as e:
                    print(f"Lỗi ghi file {filename}: {e}")

        
    print(f"✅ HOÀN TẤT! Đã sinh tổng cộng {total_files} file trong {LOCAL_SOURCE_DIR}.")

if __name__ == "__main__":
    generate_batch_data()