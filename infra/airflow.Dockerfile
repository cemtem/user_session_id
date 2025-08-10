FROM apache/airflow:2.10.2-python3.11

USER root
RUN apt-get update && apt-get install -y curl openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN curl -fsSL https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz \
  | tar -xz -C /opt && ln -s /opt/spark-3.5.1-bin-hadoop3 /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==5.2.0 delta-spark==3.2.0
