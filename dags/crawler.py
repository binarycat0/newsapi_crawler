import os
import time
from datetime import datetime, timedelta

import redis
import requests
from airflow import DAG, AirflowException
from airflow.hooks.http_hook import HttpHook
from airflow.logging_config import log
from airflow.operators.http_operator import SimpleHttpOperator
from google.api_core.exceptions import DeadlineExceeded
from google.cloud import firestore
from google.cloud.client import ClientWithProject
from pymongo import MongoClient

NEWSAPI_TOKEN = open(os.environ.get('NEWSAPI_TOKEN_FILE'), 'r').read()

MONGO_HOST = os.environ.get('MONGO_HOST')
MONGO_PASSWORD = os.environ.get('MONGO_PASSWORD')
MONGO_USER = os.environ.get('MONGO_USER')

REDIS_HOST = os.environ.get('REDIS_HOST')
REDIS_PORT = os.environ.get('REDIS_PORT')

GOOGLE_CLOUD_TIMEOUT = os.environ.get('GOOGLE_CLOUD_TIMEOUT')
GOOGLE_CLOUD_REQUEST_LIMIT = os.environ.get('GOOGLE_CLOUD_REQUEST_LIMIT')

NEWSAPI_URL = 'https://newsapi.org/v2/everything'

MONGO_URI = f'mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}'

log.debug(f'NEWSAPI_TOKEN: {NEWSAPI_TOKEN}')

COLLECTION_SOURCES_KEY = 'sources'
GOOGLE_TIMEOUT_KEY = 'GOOGLE_TIMEOUT'

# start date
_now = datetime.now()
start_date = datetime(_now.year, _now.month, _now.day)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date - timedelta(days=2),
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

default_dag = DAG('newsapi_crawler', default_args=default_args, schedule_interval=timedelta(days=1), )


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
        raise NewsApiError(f'{err_code} {err_message}')


def get_redis_client() -> redis.Redis:
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT)


def get_google_timeout():
    redis_client = get_redis_client()
    return redis_client.get(GOOGLE_TIMEOUT_KEY)


def set_google_timeout():
    redis_client = get_redis_client()
    return redis_client.set(GOOGLE_TIMEOUT_KEY, GOOGLE_CLOUD_TIMEOUT, 1)


def _get_google_cloud_client(google_cloud_module) -> ClientWithProject:
    # check timeout
    if get_google_timeout():
        log.info('waiting for GOOGLE TIMEOUT')
        time.sleep(GOOGLE_CLOUD_TIMEOUT)

    client = None
    while client is None:
        try:
            client = google_cloud_module.Client()
        except DeadlineExceeded:
            log.info('google DeadlineExceeded exception')
            time.sleep(GOOGLE_CLOUD_TIMEOUT)

    # set timeout
    set_google_timeout()

    return client


def get_store_client() -> firestore.Client:
    store_client = _get_google_cloud_client(firestore)
    return store_client


def get_mongo_client() -> MongoClient:
    mongo_client = MongoClient(MONGO_URI)
    return mongo_client


def save_to_mongo(collection: str, data: list):
    mongo_client = get_mongo_client()

    airflow_db = mongo_client.get_database('airflow')
    sources_collection = airflow_db.get_collection(collection)
    sources_collection.insert_many(data)


class NewsApiError(AirflowException): ...


class _SimpleHttpOperator(SimpleHttpOperator):
    task_id = None
    execution_date: datetime

    def __init__(self, *args, **kwargs):
        super().__init__(task_id=self.task_id, dag=default_dag, *args, **kwargs)

    def execute(self, context: dict) -> requests.Response:
        setattr(self, 'execution_date', context.get('execution_date'))
        log.info(f'execution_date: {self.execution_date}')

        http = HttpHook(self.method, http_conn_id=self.http_conn_id)

        self.log.info("Calling HTTP method")

        response = http.run(self.endpoint,
                            self.data,
                            self.headers,
                            self.extra_options)
        if self.log_response:
            self.log.info(response.text)
        if self.response_check:
            if not self.response_check(response):
                raise AirflowException("Response check returned False.")
        if self.xcom_push_flag:
            return response.text

        return response


class PublishEventsToPubSub(_SimpleHttpOperator):
    task_id = 'publish_events_to_pub_sub'


class LoadsDataToBigQuery(_SimpleHttpOperator):
    task_id = 'loads_data_to_big_query'


class StoreDataToGoogleDrive(_SimpleHttpOperator):
    task_id = 'store_data_to_google_drive'


class GetData(_SimpleHttpOperator):
    task_id = 'get_data_api'

    def execute(self, context):
        return super().execute(context)


############
# sources


class PublishNewsSources(_SimpleHttpOperator):
    task_id = 'publish_news_sources'


class LoadsNewsSources(_SimpleHttpOperator):
    task_id = 'loads_news_sources'


class StoreNewsSources(_SimpleHttpOperator):
    task_id = 'store_news_sources'


class GetNewsSources(_SimpleHttpOperator):
    task_id = 'get_news_sources'

    def execute(self, context):
        response = super().execute(context)

        data = response.json()
        sources = data.get('sources')
        log.info(f'sources_count: {len(sources)}')
        execution_date = self.execution_date.date().isoformat()

        # get sore client
        firestore_client = get_store_client()

        # get collection
        sources_ref = firestore_client.collection(f'sources-{execution_date}')

        for source in sources:
            # save documents
            sources_ref.add(source, source.get('id'))


publish_news_sources = PublishNewsSources()
loads_news_sources = LoadsNewsSources()
store_news_sources = StoreNewsSources()
get_news_sources = GetNewsSources(endpoint=f'v2/{COLLECTION_SOURCES_KEY}', response_check=_response_check)

publish_news_sources.set_upstream(loads_news_sources)
loads_news_sources.set_upstream(store_news_sources)
store_news_sources.set_upstream(get_news_sources)


# sources
############


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
get_data_task.set_upstream(publish_news_sources)

check_schemas_task.set_upstream(get_schemas_task)

get_news_sources.set_upstream(ping_task)
get_schemas_task.set_upstream(ping_task)
