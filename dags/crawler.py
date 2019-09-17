import abc
import json
import os
import time
from datetime import datetime, timedelta
from distutils.util import strtobool

import redis
import requests
from airflow import DAG, AirflowException
from airflow.hooks.http_hook import HttpHook
from airflow.logging_config import log
from airflow.operators import BaseOperator
from airflow.operators.http_operator import SimpleHttpOperator
from google.api_core.exceptions import DeadlineExceeded
from google.cloud import firestore, pubsub_v1
from google.cloud.client import ClientWithProject
from google.cloud.pubsub_v1 import publisher
from newsapi import NewsApiClient
from pymongo import MongoClient

NEWSAPI_TOKEN = open(os.environ.get('NEWSAPI_TOKEN_FILE'), 'r').read()
NEWSAPI_DEV_MODE = strtobool(os.environ.get('NEWSAPI_DEV_MODE'))

MONGO_HOST = os.environ.get('MONGO_HOST')
MONGO_PORT = int(os.environ.get('MONGO_PORT'))
MONGO_PASSWORD = os.environ.get('MONGO_PASSWORD')
MONGO_USER = os.environ.get('MONGO_USER')

REDIS_HOST = os.environ.get('REDIS_HOST')
REDIS_PORT = int(os.environ.get('REDIS_PORT'))

GOOGLE_CLOUD_PROJECT = open(os.environ.get('GOOGLE_CLOUD_PROJECT_FILE'), 'r').read()
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

GOOGLE_CLOUD_TIMEOUT = int(os.environ.get('GOOGLE_CLOUD_TIMEOUT'))
GOOGLE_CLOUD_REQUEST_LIMIT = int(os.environ.get('GOOGLE_CLOUD_REQUEST_LIMIT'))

NEWSAPI_URL = 'https://newsapi.org/v2/everything'

MONGO_URI = f'mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}'

log.debug(f'NEWSAPI_TOKEN: {NEWSAPI_TOKEN}')

COLLECTION_SOURCES_KEY = 'sources'
SOURCES_SCHEMA_SOURCES_KEY = 'sources-schema'
ARTICLES_SCHEMA_SOURCES_KEY = 'articles-schema'
GOOGLE_TIMEOUT_KEY = 'GOOGLE_TIMEOUT'

# start date
_now = datetime.now()
start_date = datetime(_now.year, _now.month, _now.day)

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
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


def get_publisher_client() -> pubsub_v1.PublisherClient:
    publisher_client = _get_google_cloud_client(publisher)
    return publisher_client


def get_mongo_client() -> MongoClient:
    mongo_client = MongoClient(MONGO_URI)
    return mongo_client


def save_to_mongo(collection: str, data: list):
    mongo_client = get_mongo_client()

    airflow_db = mongo_client.get_database('airflow')
    sources_collection = airflow_db.get_collection(collection)
    sources_collection.insert_many(data)


def get_google_store_source_key(date: str):
    return f'{COLLECTION_SOURCES_KEY}-{date}'


def get_google_store_source_schema_key(date: str):
    return f'source-schema-{date}'


def get_google_store_article_schema_key(date: str):
    return f'article-schema-{date}'


def get_newsapi_client() -> NewsApiClient:
    return NewsApiClient(NEWSAPI_TOKEN)


def check_pubsub_topic(pubsub_client: pubsub_v1.PublisherClient, topic):
    # check topic in pubsub
    project_path = pubsub_client.project_path(GOOGLE_CLOUD_PROJECT)
    topics = [topic_ref.name for topic_ref in pubsub_client.list_topics(project_path)]
    if topic not in topics:
        log.info(f'create pubsub topic {topic}')
        pubsub_client.create_topic(topic)


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


class GetData(_SimpleHttpOperator):
    task_id = 'get_data_api'

    def execute(self, context):
        return super().execute(context)


############
# sources

class _BaseOperator(BaseOperator):
    task_id: str
    execution_date: datetime
    prev_execution_date: datetime

    def __init__(self, *args, **kwargs):
        super().__init__(task_id=self.task_id, dag=default_dag, *args, **kwargs)

    def execute(self, context: dict):
        setattr(self, 'execution_date', context.get('execution_date'))
        setattr(self, 'prev_execution_date', (context.get('execution_date') - timedelta(days=1)))

        return self.execute_(context)

    @abc.abstractmethod
    def execute_(self, context: dict):
        pass


class CheckAndPublishNewsSources(_BaseOperator):
    task_id = 'publish_news_sources'

    def execute_(self, context: dict):
        # get pubsub client
        pubsub_client = get_publisher_client()
        topic = pubsub_client.topic_path(GOOGLE_CLOUD_PROJECT, COLLECTION_SOURCES_KEY)

        # check topic in pubsub
        check_pubsub_topic(pubsub_client, topic)

        # get sore client
        firestore_client = get_store_client()

        execution_date = self.execution_date.date().isoformat()
        prev_execution_date = self.prev_execution_date.isoformat()

        # get previous news_sources
        current_sources_ref = firestore_client.collection(get_google_store_source_key(execution_date))
        prev_sources_ref = firestore_client.collection(get_google_store_source_key(prev_execution_date))
        prev_keys = list(source_ref.to_dict()['id'] for source_ref in prev_sources_ref.select(['id']).stream())

        for current_source_ref in current_sources_ref.stream():
            source = current_source_ref.to_dict()

            # source id is new
            if source['id'] not in prev_keys:
                pubsub_client.publish(topic, bytes(json.dumps(source), 'utf8'))


class GetNewsSources(_BaseOperator):
    task_id = 'get_news_sources'

    def execute_(self, context: dict):
        newsapi_client = get_newsapi_client()
        sources = newsapi_client.get_sources()['sources']

        #
        log.info(f'sources_count: {len(sources)}')
        execution_date = self.execution_date.date().isoformat()

        # get sore client
        firestore_client = get_store_client()

        # get collection
        sources_ref = firestore_client.collection(get_google_store_source_key(execution_date))

        for source in sources:
            # save documents
            if sources_ref.document(source.get('id')).get().exists:
                sources_ref.document(source.get('id')).update(source)
            else:
                sources_ref.add(source, source.get('id'))


publish_news_sources = CheckAndPublishNewsSources()
get_news_sources = GetNewsSources()

publish_news_sources.set_upstream(get_news_sources)


# sources
############


class CheckDataSchemas(_BaseOperator):
    task_id = 'check_data_schemas'

    def execute_(self, context):
        execution_date = self.execution_date.date().isoformat()
        prev_execution_date = self.prev_execution_date.isoformat()

        firestore_client = get_store_client()

        sources_schema_ref = firestore_client.collection('sources_schema')
        article_schema_ref = firestore_client.collection('article_schema')

        # get pubsub client
        pubsub_client = get_publisher_client()
        sources_schema_topic = pubsub_client.topic_path(GOOGLE_CLOUD_PROJECT, SOURCES_SCHEMA_SOURCES_KEY)
        articles_schema_topic = pubsub_client.topic_path(GOOGLE_CLOUD_PROJECT, ARTICLES_SCHEMA_SOURCES_KEY)

        # check topic in pubsub
        check_pubsub_topic(pubsub_client, sources_schema_topic)
        check_pubsub_topic(pubsub_client, articles_schema_topic)

        # source schema
        prev_sources_schema = sources_schema_ref.document(prev_execution_date).get()
        sources_schema = sources_schema_ref.document(execution_date).get()

        if prev_sources_schema.exists and sources_schema.exists:

            remove = set(prev_sources_schema.to_dict()['keys']).difference(set(sources_schema.to_dict()['keys']))
            new = set(sources_schema.to_dict()['keys']).difference(set(prev_sources_schema.to_dict()['keys']))

            if len(remove):
                pubsub_client.publish(sources_schema_topic, bytes(json.dumps({'remove': list(remove)}), 'utf8'))
            if len(new):
                pubsub_client.publish(sources_schema_topic, bytes(json.dumps({'new': list(new)}), 'utf8'))
        elif sources_schema.exists:
            pubsub_client.publish(
                articles_schema_topic,
                bytes(json.dumps({'new': list(sources_schema.to_dict()['keys'])}), 'utf8')
            )

        # article schema
        prev_article_schema = article_schema_ref.document(prev_execution_date).get()
        article_schema = article_schema_ref.document(execution_date).get()

        if prev_article_schema.exists and article_schema.exists:

            remove = set(prev_article_schema.to_dict()['keys']).difference(set(article_schema.to_dict()['keys']))
            new = set(article_schema.to_dict()['keys']).difference(set(prev_article_schema.to_dict()['keys']))

            if len(remove):
                pubsub_client.publish(articles_schema_topic, bytes(json.dumps({'remove': list(remove)}), 'utf8'))
            if len(new):
                pubsub_client.publish(articles_schema_topic, bytes(json.dumps({'new': list(new)}), 'utf8'))
        elif article_schema.exists:
            pubsub_client.publish(
                articles_schema_topic,
                bytes(json.dumps({'new': list(article_schema.to_dict()['keys'])}), 'utf8')
            )


class GetDataSchemas(_BaseOperator):
    task_id = 'get_data_schemas'

    def execute_(self, context):
        execution_date = self.execution_date.date().isoformat()

        client = get_newsapi_client()
        firestore_client = get_store_client()

        sources = client.get_sources(language='en').get('sources')
        top_headlines = client.get_top_headlines(page_size=1, page=1).get('articles')

        source = sources.pop()
        headline = top_headlines.pop()

        source_keys = list(source.keys())
        headline_keys = list(headline.keys())

        sources_schema_ref = firestore_client.collection('sources_schema')
        article_schema_ref = firestore_client.collection('article_schema')

        sources_schema_ref.document(execution_date).set({'keys': source_keys})
        article_schema_ref.document(execution_date).set({'keys': headline_keys})


class PingNewsApiOperator(_BaseOperator):
    task_id = 'ping_news_api'

    def execute_(self, context):
        get_newsapi_client()


#
ping_task = PingNewsApiOperator()
get_schemas_task = GetDataSchemas()
check_schemas_task = CheckDataSchemas()
get_data_task = GetData()
loads_data_task = LoadsDataToBigQuery()
publish_data_task = PublishEventsToPubSub()

#
publish_data_task.set_upstream(loads_data_task)
loads_data_task.set_upstream(get_data_task)
get_data_task.set_upstream(check_schemas_task)
get_data_task.set_upstream(publish_news_sources)

check_schemas_task.set_upstream(get_schemas_task)

get_news_sources.set_upstream(ping_task)
get_schemas_task.set_upstream(ping_task)
