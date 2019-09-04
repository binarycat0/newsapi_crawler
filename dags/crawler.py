import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.logging_config import log
from airflow.operators.http_operator import SimpleHttpOperator

NEWSAPI_URL = 'https://newsapi.org/v2/everything'

NEWSAPI_TOKEN = os.environ.get('NEWSAPI_TOKEN')
log.debug(f'NEWSAPI_TOKEN: {NEWSAPI_TOKEN}')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(days=2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}

newsapi_crawler_dag = DAG(
    'newsapi_crawler',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)


class NewsApiError(Exception): ...


class PingNewsApiOperator(SimpleHttpOperator):

    def execute(self, context):
        log.info(f'start_date: {self.start_date}')
        log.info(f'end_date: {self.end_date}')
        log.info(f'execution_date: {context.get("execution_date")}')
        return super().execute(context)


class CrawlerOperator(SimpleHttpOperator):
    query = 'bitcoin'

    def execute(self, context):
        return super().execute(context)


def _response_check(response: requests.Response):
    log.info(f'response_code: {response.status_code}')
    if response.status_code:
        return True


#
news_bitcoin_task = CrawlerOperator(
    task_id='news_bitcoin',
    dag=newsapi_crawler_dag,
    method='GET',
    headers={'Authorization': f'Bearer {NEWSAPI_TOKEN}'},
    response_check=_response_check,
    log_response=False,
    http_conn_id='newasapi',
    endpoint='/'
)

# upstreams
ping_task = PingNewsApiOperator(
    task_id='ping_newasapi',
    dag=newsapi_crawler_dag,
    method='GET',
    headers={'Authorization': f'Bearer {NEWSAPI_TOKEN}'},
    response_check=_response_check,
    log_response=False,
    http_conn_id='newasapi',
    endpoint='/'
)
news_bitcoin_task.set_upstream(ping_task)
