# Sử dụng image nền CÙNG PHIÊN BẢN với cluster Hadoop (3.2.1)
FROM bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8

# Cài đặt Sqoop và trình điều khiển PostgreSQL
ENV SQOOP_VERSION=1.4.7
ENV SQOOP_HOME=/opt/sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0
ENV PATH=$PATH:$SQOOP_HOME/bin

# Cập nhật người dùng thành root để có quyền cài đặt
USER root

# ---- SỬA LỖI APT-GET CHO DEBIAN CŨ ----
# Thay đổi địa chỉ kho phần mềm sang server lưu trữ (archive)
RUN sed -i 's/deb.debian.org/archive.debian.org/g' /etc/apt/sources.list && \
    sed -i 's/security.debian.org/archive.debian.org/g' /etc/apt/sources.list && \
    sed -i '/stretch-updates/d' /etc/apt/sources.list
# ------------------------------------

# Tải và cài đặt các gói cần thiết
RUN apt-get update && \
    apt-get install -y --no-install-recommends wget procps && \
    wget https://archive.apache.org/dist/sqoop/${SQOOP_VERSION}/sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz -O /tmp/sqoop.tar.gz && \
    tar -xzvf /tmp/sqoop.tar.gz -C /opt/ && \
    rm /tmp/sqoop.tar.gz && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Sao chép trình điều khiển JDBC vào thư mục lib của Sqoop
COPY lib/postgresql-*.jar ${SQOOP_HOME}/lib/

# Giữ container chạy để có thể truy cập vào
CMD ["tail", "-f", "/dev/null"]