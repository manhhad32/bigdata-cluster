import csv
import random
import os
import time
from datetime import datetime, timedelta

# --- CẤU HÌNH ---
LOCAL_SOURCE_DIR = "/Users/nguyenmanhha/Desktop/data"
SHOPS = 60
DAYS_TO_GENERATE = 360
HOURS_PER_DAY = 18

# --- THAM SỐ MONG MUỐN ---
TARGET_TOTAL_FILES = 50000

PRODUCTS = [
    (100, "Cà phê Mocha Đá", 39000),
    (101, "Cà phê sữa", 39000),
    (102, "Espresso", 59000),
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

    total_hours_in_year = DAYS_TO_GENERATE * HOURS_PER_DAY
    avg_files_per_hour = TARGET_TOTAL_FILES / total_hours_in_year
    base_files = int(avg_files_per_hour)
    remainder_prob = avg_files_per_hour - base_files

    print(f"--- Bắt đầu sinh dữ liệu ---")
    print(f"Mục tiêu: {TARGET_TOTAL_FILES} file.")
    
    start_date = datetime(2023, 1, 1)
    total_files_created = 0
    
    # Biến cờ để thoát vòng lặp ngoài cùng khi đã đủ chỉ tiêu
    target_reached = False

    last_day_index = DAYS_TO_GENERATE - 1
    last_hour_val = 6 + HOURS_PER_DAY - 1

    for day in range(DAYS_TO_GENERATE):
        if target_reached: break # Thoát vòng lặp ngày nếu đã đủ

        current_date = start_date + timedelta(days=day)
        date_str = current_date.strftime('%Y%m%d')

        for h in range(6, 6 + HOURS_PER_DAY):
            # 1. KIỂM TRA ĐIỀU KIỆN DỪNG SỚM
            remaining_slots = TARGET_TOTAL_FILES - total_files_created
            
            if remaining_slots <= 0:
                print(f"✅ Đã đạt đủ {TARGET_TOTAL_FILES} file tại ngày {date_str} lúc {h}h. Dừng lại.")
                target_reached = True
                break # Thoát vòng lặp giờ

            hour_str = f"{h:02d}"
            is_last_moment = (day == last_day_index) and (h == last_hour_val)

            # 2. TÍNH SỐ LƯỢNG CẦN SINH TRONG GIỜ NÀY
            if is_last_moment:
                # Giờ cuối cùng: Sinh toàn bộ số còn thiếu
                num_files_this_hour = remaining_slots
                print(f"🏁 Giờ chót ({date_str}-{hour_str}): Sinh nốt {num_files_this_hour} file.")
            else:
                # Giờ bình thường: Sinh theo xác suất trung bình
                num_files_this_hour = base_files
                if random.random() < remainder_prob:
                    num_files_this_hour += 1

                # 3. QUAN TRỌNG: CẮT GỌT (CLIPPING)
                # Nếu số định sinh > số còn thiếu -> Chỉ sinh số còn thiếu
                if num_files_this_hour > remaining_slots:
                    num_files_this_hour = remaining_slots

            # --- SINH FILE ---
            if num_files_this_hour > 0:
                # Đảm bảo không sample quá số lượng shop có sẵn
                # (Trường hợp còn thiếu 100 file ở giờ chót nhưng chỉ có 60 shop -> code sẽ chỉ sinh 60 file và báo thiếu, tránh crash)
                safe_sample_count = min(num_files_this_hour, SHOPS)
                
                active_shops = random.sample(range(1, SHOPS + 1), safe_sample_count)

                for shop_id in active_shops:
                    filename = f"Shop-{shop_id}-{date_str}-{hour_str}.csv"
                    filepath = os.path.join(LOCAL_SOURCE_DIR, filename)

                    data = []
                    total_items = random.randint(5, 50)
                    for _ in range(total_items):
                        order_id = int(f"{date_str}{random.randint(1000, 9999)}")
                        prod = random.choice(PRODUCTS)
                        amount = random.randint(1, 10)
                        discount = int(prod[2] * amount * 0.1) if random.random() < 0.1 else 0
                        data.append([order_id, prod[0], prod[1], amount, prod[2], discount])

                    try:
                        with open(filepath, 'w', newline='', encoding='utf-8') as f:
                            csv.writer(f).writerows(data)
                        total_files_created += 1
                    except Exception:
                        pass
        
        # Log tiến độ nhẹ
        if day % 20 == 0:
            print(f"-> Ngày {day}: {total_files_created}/{TARGET_TOTAL_FILES}")

    print(f"✅ HOÀN TẤT! Tổng số file: {total_files_created} / {TARGET_TOTAL_FILES}")

if __name__ == "__main__":
    generate_batch_data()