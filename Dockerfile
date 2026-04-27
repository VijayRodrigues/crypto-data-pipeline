FROM prefecthq/prefect:3-latest

USER root

RUN apt-get update && apt-get install -y default-jdk

ENV JAVA_HOME=/usr/lib/jvm/default-java

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .