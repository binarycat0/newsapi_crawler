#!/bin/sh
airflow initdb
airflow webserver -w 1 -p 8080