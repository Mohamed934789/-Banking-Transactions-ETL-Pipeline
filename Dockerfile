FROM apache/airflow:2.10.4
USER root
RUN apt-get update -qq && apt-get install -y --no-install-recommends openjdk-17-jdk-headless && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
USER airflow
RUN pip install --no-cache-dir "apache-airflow==2.10.4" "pyspark==3.5.5" "pandas==2.2.3" "snowflake-connector-python[pandas]==3.14.0" "requests==2.32.3"
