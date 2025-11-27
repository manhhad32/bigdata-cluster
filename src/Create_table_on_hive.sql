-- Bước 1: Tạo Database quản lý bán hàng
CREATE DATABASE IF NOT EXISTS sales_db;

-- Bước 2: Chuyển sang sử dụng DB này
USE sales_db;

-- Bước 3: Tạo bảng EXTERNAL liên kết trực tiếp với thư mục /data trên HDFS
-- Lưu ý: Định nghĩa kiểu dữ liệu khớp với file CSV
CREATE EXTERNAL TABLE IF NOT EXISTS Orders (
    OrderID BIGINT,
    ProductID INT,
    ProductName STRING,
    Amount INT,
    Price DOUBLE,
    Discount DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data';

-- Câu sql kiểm tra xem có dữ liệu hay chưa:
SELECT * FROM sales_db.orders LIMIT 20;