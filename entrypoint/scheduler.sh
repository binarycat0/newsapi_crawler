#!/bin/sh
airflow upgradedb && \
airflow connections -a --conn_id=newsapi --conn_uri=https://newsapi.org/

rm -rf $AIRFLOW_HOME/airflow-flower.pid
rm -rf $AIRFLOW_HOME/airflow-scheduler.pid
rm -rf $AIRFLOW_HOME/airflow-worker.pid

set -m

airflow flower --pid airflow-flower.pid -p 8081 &
airflow scheduler --pid airflow-scheduler.pid &
airflow worker --pid airflow-worker.pid

fg %1