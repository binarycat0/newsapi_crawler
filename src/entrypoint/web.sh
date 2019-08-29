#!/bin/sh
airflow initdb
airflow upgradedb

set -m

airflow webserver -p 8080 &
airflow flower -p 8081 &
airflow scheduler &
airflow worker
