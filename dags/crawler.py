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

    'method': 'GET',
    'headers': {'Authorization': f'Bearer {NEWSAPI_TOKEN}'},
    'http_conn_id': 'newsapi',
    'endpoint': '/',
    'log_response': False,
}

default_dag = DAG(
    'newsapi_crawler',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)


class NewsApiError(Exception): ...


class _SimpleHttpOperator(SimpleHttpOperator):
    task_id = None

    def __init__(self, *args, **kwargs):
        super().__init__(task_id=self.task_id, dag=default_dag, *args, **kwargs)


class PublishEventsToPubSub(_SimpleHttpOperator):
    task_id = 'publish_events_to_pub_sub'


class LoadsDataToBigQuery(_SimpleHttpOperator):
    task_id = 'loads_data_to_big_query'


class StoreDataToGoogleDrive(_SimpleHttpOperator):
    task_id = 'store_data_to_google_drive'


class GetData(_SimpleHttpOperator):
    task_id = 'get_data_api'

    def execute(self, context):
        execution_date = context.get("execution_date")
        return super().execute(context)


class CheckDataSchemas(_SimpleHttpOperator):
    task_id = 'check_data_schemas'


class GetDataSchemas(_SimpleHttpOperator):
    task_id = 'get_data_schemas'


class PingNewsApiOperator(_SimpleHttpOperator):
    task_id = 'ping_news_api'

    def execute(self, context):
        log.info(f'start_date: {self.start_date}')
        log.info(f'end_date: {self.end_date}')
        log.info(f'execution_date: {context.get("execution_date")}')
        return super().execute(context)


def _response_check(response: requests.Response):
    log.info(f'response_code: {response.status_code}')

    data = None
    try:
        data = response.json()
    except:
        pass

    data = data or {}

    if response.status_code == 200:
        return True
    else:
        err_code = data.get('code')
        err_message = data.get('message')

        log.error(f'{err_code} {err_message}')
        raise NewsApiError()


#
ping_task = PingNewsApiOperator()
get_schemas_task = GetDataSchemas()
check_schemas_task = CheckDataSchemas()
get_data_task = GetData()
store_data_task = StoreDataToGoogleDrive()
loads_data_task = LoadsDataToBigQuery()
publish_data_task = PublishEventsToPubSub()

#
publish_data_task.set_upstream(loads_data_task)
loads_data_task.set_upstream(store_data_task)
store_data_task.set_upstream(get_data_task)
get_data_task.set_upstream(check_schemas_task)
check_schemas_task.set_upstream(get_schemas_task)
get_schemas_task.set_upstream(ping_task)
