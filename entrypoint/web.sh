#!/bin/sh
airflow upgradedb

rm -rf $AIRFLOW_HOME/airflow-webserver.pid

set -m

airflow webserver -p 8080 &
airflow flower -p 8081 &
airflow scheduler &
airflow worker

fg %1