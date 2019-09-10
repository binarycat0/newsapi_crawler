#!/bin/sh
airflow upgradedb && \
airflow connections -a --conn_id=newsapi --conn_uri=https://newsapi.org/

set -m

airflow flower -p 8081 &
airflow scheduler &
airflow worker

fg %1