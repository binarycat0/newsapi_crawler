#!/bin/sh
airflow upgradedb && \
airflow connections -a --conn_id=newsapi --conn_uri=https://newsapi.org/

rm -rf $AIRFLOW_HOME/airflow-webserver.pid

airflow webserver -p 8080
