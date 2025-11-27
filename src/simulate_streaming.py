import os
import time
import shutil
import random

# --- CẤU HÌNH ---
# Thư mục chứa 388.800 file (Nguồn)
LOCAL_SOURCE_DIR = "/Users/nguyenmanhha/Desktop/data" 

# Thư mục NiFi đang lắng nghe (Đích)
# Lưu ý: Thư mục này phải map với volume của NiFi trong docker-compose
LOCAL_DATA_REALTIME = "./realtime-data" 

# --- THAM SỐ COPY ---
N = 60  # Số lượng file copy mỗi lần (Ví dụ: 60 file tương ứng dữ liệu 1 giờ của 60 shop)
T = 5   # Khoảng thời gian nghỉ giữa các lần copy (giây)

def simulate_realtime_data():
    # 1. Kiểm tra thư mục
    if not os.path.exists(LOCAL_SOURCE_DIR):
        print(f"Lỗi: Không tìm thấy thư mục nguồn {LOCAL_SOURCE_DIR}. Hãy chạy file gen_data.py trước!")
        return
    
    if not os.path.exists(LOCAL_DATA_REALTIME):
        os.makedirs(LOCAL_DATA_REALTIME)
        print(f"Đã tạo thư mục đích: {LOCAL_DATA_REALTIME}")

    # 2. Lấy danh sách toàn bộ file và sắp xếp
    print("Đang đọc danh sách file từ kho lưu trữ...")
    all_files = [f for f in os.listdir(LOCAL_SOURCE_DIR) if f.endswith('.csv')]
    
    # Sắp xếp để copy đúng thứ tự thời gian (Ngày cũ copy trước)
    # Tên file Shop-ID-YYYYMMDD... nên cần sort khéo một chút hoặc sort mặc định string cũng tạm ổn
    # Để chuẩn nhất, ta sort theo cụm thời gian trong tên file
    all_files.sort() 
    
    total_files = len(all_files)
    print(f"Tìm thấy {total_files} file trong kho. Bắt đầu giả lập stream...")

    # 3. Vòng lặp copy từng batch (N files)
    current_index = 0
    
    while current_index < total_files:
        # Lấy ra N file tiếp theo
        batch_files = all_files[current_index : current_index + N]
        
        if not batch_files:
            break
            
        print(f"--- Đang copy {len(batch_files)} file (Batch {current_index // N + 1}) ---")
        
        for filename in batch_files:
            src_path = os.path.join(LOCAL_SOURCE_DIR, filename)
            dst_path = os.path.join(LOCAL_DATA_REALTIME, filename)
            
            try:
                # Dùng copy thay vì move để giữ nguyên dữ liệu gốc cho lần test sau
                shutil.copy(src_path, dst_path)
            except Exception as e:
                print(f"Lỗi copy file {filename}: {e}")
        
        # Cập nhật chỉ số
        current_index += len(batch_files)
        
        # Nghỉ T giây
        print(f"-> Đã đẩy xong batch. Chờ {T} giây...")
        time.sleep(T)

    print("✅ ĐÃ COPY TOÀN BỘ DỮ LIỆU SANG HỆ THỐNG REALTIME.")

if __name__ == "__main__":
    simulate_realtime_data()