# newsapi_crawler


## Required:

- docker
    - [Mac OS](https://docs.docker.com/docker-for-mac/install/)
    - [Windows](https://docs.docker.com/docker-for-windows/install/)
    - [Ubuntu](https://docs.docker.com/install/linux/docker-ce/ubuntu/#install-docker-ce)
    - [Centos](https://docs.docker.com/install/linux/docker-ce/centos/#install-docker-ce)
- docker-compose
    - [Install Docker Compose](https://docs.docker.com/compose/install/)
    
    
## start

after start default ports:

- apache_airflow web
    - 8080
- apache_airflow flower
    - 8081

### with docker-compose

simple way to run app

    docker-compose build
    docker-compose up
    
    
### with docker

before run app please edit ./src/airflow.cfg

    docker build . -t airflow:latest
    docker run -i -t -P \
        -p 8080:8080 \
        -v ./src/airflow.cfg:/app/airflow/ \
        -e "AIRFLOW_HOME=/app/airflow"
        --name=newsapi_crawler
        airflow:latest
