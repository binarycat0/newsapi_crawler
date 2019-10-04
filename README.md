# newsapi_crawler
Scheduled tasks get articles from the newsapi.org and store them to the BigQuery - Google Cloud Platform (GCP).

- getting sources from newsapi.org
    - store them to Firestore-GCP separated by date to `/<your_projec_id>/sources/<task_date>`
    - store Source schema to `/<your_projec_id>/sources_schema/<task_date>`
    - compare Source schemas for current and previous dates and report differences (new or removed) to PubSub-GCP for topic `projects/<your_projec_id>/topics/sources-schema`
    - delete `sources_schema` older 2 days
- getting first top artice for storing article_schema
    - store them to Firestore-GCP separated by date to `/<your_projec_id>/article_schema/<task_date>`
    - compare Article schemas and report to PubSub-GCP for topic `projects/quick-pathway-251909/topics/articles-schema`
    - delete `article_schema` older 2 days
    - getting articles and store them to Firestore-GCP separated by date and keyword to `/<your_projec_id>/article/<task_date>/<keyword>`
- copy articles to BigQuery-GCP to datasource `newsapi` to table `articles`
    - table `/<your_projec_id>/newsapi/articles`

Keywords for queryes - `/keys/newsapi_query_keywords.txt`


## Required:

- docker
    - [Mac OS](https://docs.docker.com/docker-for-mac/install/)
    - [Windows](https://docs.docker.com/docker-for-windows/install/)
    - [Ubuntu](https://docs.docker.com/install/linux/docker-ce/ubuntu/#install-docker-ce)
    - [Centos](https://docs.docker.com/install/linux/docker-ce/centos/#install-docker-ce)
- docker-compose
    - [Install Docker Compose](https://docs.docker.com/compose/install/)

## Configure
    
### google cloud settings

- credentials
    - You must create a Service Key in the Google Cloud Platform with correct credentials, download key and copy key's content to the file `/keys/google_cloud_key.json`
- project name
    - insert your project name into `/keys/google_cloud_project.txt`
- big query table schema
    - if you need change default schema in `/keys/big_query_articles_schema.json`

### newsapi.org settings

- token
    - Create account on newsapi.org and copy your `TOKEN` to the file  `/keys/newsapi_token.txt`
- keywords 
    - put your keywords into `/keys/newsapi_query_keywords.txt`. separated by new_line
    
## Start

Simple way to build
        
    # clone
    git clone git@github.com:catbinary/newsapi_crawler.git ~/newsapi_crawler
    cd ~/newsapi_crawler
    
    # build
    docker-compose build
    
    # start
    docker-compose up

after start services will be awailable:

- apache_airflow web
    - http://localhost:8080/
- apache_airflow flower
    - http://localhost:8081/

