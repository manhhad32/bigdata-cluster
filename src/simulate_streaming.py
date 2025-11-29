import os
import time
import shutil

# --- CẤU HÌNH ---
# Thư mục chứa dữ liệu lịch sử (/home/hduser/data)
LOCAL_SOURCE_DIR = "/Users/nguyenmanhha/Desktop/data" 

# Thư mục NiFi đang lắng nghe (/home/hduser/realtime-data)
# Đây là thư mục phải map với volume của NiFi trong docker-compose
LOCAL_DATA_REALTIME = "./realtime-data" 

# --- THAM SỐ GIẢ LẬP ---
N = 500  # Số lượng file copy mỗi lần (Batch size)
T = 2   # Khoảng thời gian nghỉ giữa các lần copy (giây)

def simulate_realtime_data():
    print("--- KHỞI ĐỘNG CHƯƠNG TRÌNH GIẢ LẬP STREAMING ---")

    # 1. LOGIC MỚI: Kiểm tra và tạo thư mục ĐÍCH (LOCAL_DATA_REALTIME)
    if not os.path.exists(LOCAL_DATA_REALTIME):
        try:
            os.makedirs(LOCAL_DATA_REALTIME)
            print(f"✅ Đã tạo mới thư mục đích: {os.path.abspath(LOCAL_DATA_REALTIME)}")
        except OSError as e:
            print(f"❌ Lỗi nghiêm trọng: Không thể tạo thư mục {LOCAL_DATA_REALTIME}. Lý do: {e}")
            return
    else:
        print(f"ℹ️  Thư mục đích đã tồn tại: {os.path.abspath(LOCAL_DATA_REALTIME)}")

    # 2. Kiểm tra thư mục NGUỒN (LOCAL_SOURCE_DIR)
    if not os.path.exists(LOCAL_SOURCE_DIR):
        print(f"❌ Lỗi: Không tìm thấy thư mục nguồn '{LOCAL_SOURCE_DIR}'.")
        print("   -> Vui lòng chạy file 'gen_data.py' trước để sinh dữ liệu!")
        return

    # 3. Đọc danh sách file từ kho
    print("⏳ Đang đọc danh sách file trong kho lưu trữ...")
    try:
        all_files = [f for f in os.listdir(LOCAL_SOURCE_DIR) if f.endswith('.csv')]
        # Sắp xếp tên file để giả lập đúng trình tự thời gian (Shop-ID-YYYYMMDD...)
        all_files.sort()
    except Exception as e:
        print(f"❌ Lỗi khi đọc thư mục nguồn: {e}")
        return
    
    total_files = len(all_files)
    if total_files == 0:
        print("⚠️ Kho dữ liệu trống! Không có gì để copy.")
        return

    print(f"✅ Tìm thấy {total_files} file. Bắt đầu đẩy dữ liệu (Mỗi lần {N} file, nghỉ {T}s)...")
    print("-" * 50)

    # 4. Vòng lặp copy (Streaming Simulation)
    current_index = 0
    batch_count = 1
    
    while current_index < total_files:
        # Lấy batch N file tiếp theo
        batch_files = all_files[current_index : current_index + N]
        
        if not batch_files:
            break
            
        print(f"🔄 Batch {batch_count}: Đang đẩy {len(batch_files)} file vào hệ thống...")
        
        for filename in batch_files:
            src_path = os.path.join(LOCAL_SOURCE_DIR, filename)
            dst_path = os.path.join(LOCAL_DATA_REALTIME, filename)
            
            try:
                # Dùng shutil.copy để giữ nguyên file gốc trong kho archive
                shutil.copy(src_path, dst_path)
            except Exception as e:
                print(f"   ⚠️ Lỗi copy file {filename}: {e}")
        
        # Cập nhật tiến độ
        current_index += len(batch_files)
        batch_count += 1
        
        # Nghỉ T giây trước khi đẩy đợt tiếp theo
        print(f"   -> Hoàn tất. Chờ {T} giây...")
        time.sleep(T)

    print("-" * 50)
    print("🎉 ĐÃ COPY TOÀN BỘ DỮ LIỆU SANG HỆ THỐNG REALTIME THÀNH CÔNG.")

if __name__ == "__main__":
    simulate_realtime_data()