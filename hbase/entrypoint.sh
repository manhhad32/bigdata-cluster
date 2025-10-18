#!/bin/bash
set -e

# Đọc tham số đầu vào (sẽ là "master" hoặc "regionserver" từ docker-compose)
COMMAND=$1

# Nếu là master, khởi động master
if [ "$COMMAND" = "master" ]; then
  echo "Starting HBase Master..."
  exec "$HBASE_HOME/bin/hbase" master start
# Nếu là regionserver, khởi động regionserver
elif [ "$COMMAND" = "regionserver" ]; then
  echo "Starting HBase RegionServer..."
  exec "$HBASE_HOME/bin/hbase" regionserver start
fi

# Giữ container chạy nếu không có lệnh nào khớp
exec "$@"