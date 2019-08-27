FROM ubuntu:18.04

RUN apt update \
	&& apt install fish \
	&& apt install python3-pip \
	&& apt install python3.7 \
	&& apt install python3-dev \
	&& apt install g++ \
	&& apt install g++ psycopg2-binary \
	&& apt install postgresql-server-dev-all \
	&& apt install libpq-dev \
	&& apt clean all
