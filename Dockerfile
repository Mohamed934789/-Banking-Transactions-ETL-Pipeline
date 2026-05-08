FROM apache/airflow:2.10.4
USER airflow
RUN pip install --no-cache-dir hdfs
RUN pip install pyspark
RUN apt-get update && apt-get install -y openjdk-17-jdk

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

FROM apache/airflow:2.10.4

USER root
RUN apt-get update -qq && \
    apt-get install -y default-jdk -qq && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
RUN pip install pyspark "pandas>=2.2.0" snowflake-connector-python --quiet