# newsapi_crawler

## Required:

- docker
    - [Mac OS](https://docs.docker.com/docker-for-mac/install/)
    - [Windows](https://docs.docker.com/docker-for-windows/install/)
    - [Ubuntu](https://docs.docker.com/install/linux/docker-ce/ubuntu/#install-docker-ce)
    - [Centos](https://docs.docker.com/install/linux/docker-ce/centos/#install-docker-ce)
- docker-compose
    - [Install Docker Compose](https://docs.docker.com/compose/install/)

## Configure
    
### google cloud account 
You must create a Service Key in the Google Cloud Platform with correct credentials, download key and copy key's content to the file `/keys/google_cloud_key.json`

### newsapi.org
Create account on newsapi.org and copy your `TOKEN` to the file  `/keys/newsapi_token.txt`
    
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

