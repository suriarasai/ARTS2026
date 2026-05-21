# Dockerfile
FROM eclipse-temurin:11-jdk-jammy

# Install Python 3 and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip procps curl && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Verify installations
RUN java -version && python --version

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Create necessary directories
RUN mkdir -p /app/data /app/src /app/output /app/checkpoints

# Copy application files
COPY . .

# Set environment variables for Spark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV JAVA_HOME=/opt/java/openjdk

CMD ["bash"]