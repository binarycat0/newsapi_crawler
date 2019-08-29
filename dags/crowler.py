import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.logging_config import log
from airflow.models import BaseOperator
from airflow.operators.http_operator import SimpleHttpOperator

NEWSAPI_TOKEN = os.environ.get('NEWSAPI_TOKEN')
log.debug(f'NEWSAPI_TOKEN: {NEWSAPI_TOKEN}')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('newsapi_crowler', default_args=default_args, schedule_interval=timedelta(days=1))

t1 = SimpleHttpOperator(
    task_id='test_api',
    dag=dag,
    endpoint='/v2/everything/?q=bitcoin',
    http_conn_id='newsapi',
    headers={'Authorization': f'Bearer {NEWSAPI_TOKEN}'},
    method='GET'
)


class CrowlerOperator(BaseOperator):

    def execute(self, context):
        requests.get()
