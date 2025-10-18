# Sử dụng một image Java 8 làm nền tảng
FROM eclipse-temurin:8-jdk-focal

# Các biến môi trường cho phiên bản HBase
ENV HBASE_VERSION=2.2.3
ENV HADOOP_VERSION=3.1.2
ENV HBASE_HOME=/opt/hbase

# Tải về và giải nén HBase từ trang lưu trữ chính thức của Apache
RUN apt-get update && apt-get install -y curl && \
    curl -fSL "https://archive.apache.org/dist/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz" -o /tmp/hbase.tar.gz && \
    mkdir -p ${HBASE_HOME} && \
    tar xzf /tmp/hbase.tar.gz -C ${HBASE_HOME} --strip-components=1 && \
    rm /tmp/hbase.tar.gz && \
    apt-get purge -y --auto-remove curl

# Thêm HBase vào PATH để có thể gọi lệnh trực tiếp
ENV PATH=$HBASE_HOME/bin:$PATH

# Sao chép script khởi động vào container
COPY entrypoint.sh /
RUN chmod +x /entrypoint.sh

# Các cổng mặc định của HBase
EXPOSE 16000 16010 16020 16030

# Script sẽ được chạy khi container khởi động
ENTRYPOINT ["/entrypoint.sh"]