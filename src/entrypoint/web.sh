#!/bin/sh
airflow initdb
airflow upgradedb
airflow webserver -p 8080