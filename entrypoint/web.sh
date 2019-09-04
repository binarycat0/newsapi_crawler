#!/bin/sh
airflow upgradedb && \
airflow connections -a --conn_id=newsapi --conn_uri=https://newsapi.org/

rm -rf $AIRFLOW_HOME/airflow-webserver.pid

set -m

airflow webserver -p 8080 &
airflow flower -p 8081 &
airflow scheduler &
airflow worker

fg %1