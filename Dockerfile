FROM ubuntu:18.04

RUN apt update && apt install -y fish \
    python-pip \
    python3-pip \
    python3.7 \
    python3-dev \
    g++ \
    postgresql-server-dev-all \
    libpq-dev \
    && apt clean all

RUN mkdir -p /app
ADD ./requirements.txt /app/requirements.txt

WORKDIR /app
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt

ENV AIRFLOW_HOME /app/airflow

RUN airflow initdb

CMD airflow webserver -p 8080