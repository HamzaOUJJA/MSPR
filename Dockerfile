FROM ubuntu:latest

RUN apt-get update && apt-get install -y openjdk-17-jdk curl && apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

RUN pip install --no-cache-dir pyspark boto3 requests minio

COPY . /app
WORKDIR /app

CMD ["python", "main.py"]
