FROM ubuntu:18.04

RUN apt update && apt install -y fish \
	python3-pip \
	python3.7 \
	python3-dev \
	g++ \
	postgresql-server-dev-all \
	libpq-dev \
	&& apt clean all
