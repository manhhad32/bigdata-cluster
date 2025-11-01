import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:19092', 'localhost:19093', 'localhost:19094'],
    acks=1
)

# Kết nối đến cổng EXTERNAL đã map trong docker-compose.yml

logs = [
    "192.168.1.1 1678886400 GET /home 200",
    "10.0.0.5 1678886402 POST /login 404",
    "192.168.1.1 1678886403 GET /products 200",
    "10.0.0.2 1678886405 GET /admin 500",
    "172.16.0.4 1678886406 GET /home 200",
    "10.0.0.5 1678886407 POST /login 404",
    "10.0.0.2 1678886408 GET /admin 500",
    "192.168.1.1 1678886409 GET /home 200",
    "192.168.1.1 1678886410 GET /cart 200",
    "10.0.0.5 1678886411 POST /login 404",
    "10.0.0.2 1678886412 GET /admin 503" # Thêm 1 lỗi mới
]

print("Bắt đầu gửi log tới topic 'web_logs'...")
while True:
    log_message = random.choice(logs)
    print(f"Gửi: {log_message}")
    producer.send('web_logs', log_message.encode('utf-8'))
    time.sleep(random.uniform(0.5, 2.0)) # Gửi ngẫu nhiên 0.5-2 giây/lần