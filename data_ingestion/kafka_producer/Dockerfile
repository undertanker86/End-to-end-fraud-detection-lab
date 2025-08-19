# Sử dụng một phiên bản Flink cụ thể, ví dụ 1.17
FROM flink:1.17.1-scala_2.12

# Install Python và các thư viện hệ thống cần thiết
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Tạo symlink python (tùy chọn nhưng giữ lại cho an toàn)
RUN ln -s /usr/bin/python3 /usr/bin/python

# Cài đặt các thư viện Python
# (Bạn có thể gộp các thư viện vào file requirements.txt và cài một lần)
RUN pip3 install apache-flink==1.17.1
RUN pip3 install kafka-python==2.0.2

# Cài đặt các JAR cho Kafka connector vào /lib
RUN mkdir -p /opt/flink/lib && \
    wget -O /opt/flink/lib/flink-connector-kafka-1.17.1.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/1.17.1/flink-connector-kafka-1.17.1.jar && \
    wget -O /opt/flink/lib/kafka-clients-3.4.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar

# ----- BƯỚC QUAN TRỌNG: GIẢI QUYẾT XUNG ĐỘT PLUGIN S3 -----
# 1. Xóa plugin s3-fs-presto có sẵn trong image gốc để tránh xung đột.
# Plugin này dùng cho s3:// và có thể gây lỗi khi dùng chung với s3a://
# RUN rm -rf /opt/flink/plugins/s3-fs-presto

# 2. Cài đặt plugin s3-fs-hadoop mà chúng ta muốn sử dụng cho s3a://
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    wget -O /opt/flink/plugins/s3-fs-hadoop/flink-s3-fs-hadoop-1.17.1.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.17.1/flink-s3-fs-hadoop-1.17.1.jar
RUN rm -rf /opt/flink/plugins/s3-fs-presto
    # Sao chép code PyFlink vào image
COPY pyflink_jobs/ /opt/flink/pyflink_jobs/

# Đặt các biến môi trường cho Python
ENV PYTHONPATH=/opt/flink/pyflink_jobs:$PYTHONPATH
ENV PYTHON_EXECUTABLE=python3
ENV PYFLINK_PYTHON=python3