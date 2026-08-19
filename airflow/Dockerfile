FROM apache/airflow:2.8.1

USER root

# Install only JRE headless (not full JDK) + procps — much faster, ~120MB vs ~400MB
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Give the airflow user access to the Docker socket mounted from the host.
# We create the group with GID 999 (Docker Desktop default) and add airflow to it.
# If your Docker socket GID differs, adjust accordingly.
RUN groupadd -f -g 999 docker && usermod -aG docker airflow

# Set environment variables
# pyspark pip package provides spark-submit at its install location
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV HADOOP_CONF_DIR=/opt/hadoop/conf

# Create Hadoop conf directory (configs will be mounted via docker-compose volumes)
RUN mkdir -p /opt/hadoop/conf

USER airflow

# Install Python dependencies
# pyspark pip package bundles spark-submit — no need to download the full 400MB tarball
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Set SPARK_HOME to pyspark's bundled Spark
ENV SPARK_HOME=/home/airflow/.local/lib/python3.8/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
