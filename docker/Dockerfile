# pg_restore for the imdb db we have doesn't seem to work with the latest
# postgres
FROM postgres:9.4
RUN apt-get update
RUN apt-get install -y wget tar
ENV POSTGRES_USER=imdb
ENV POSTGRES_PASSWORD=imdb
# scripts in this directory will be run by the postgres docker init script
COPY ./init_imdb.sh /docker-entrypoint-initdb.d/
COPY ./fkindexes.sql /var/lib/postgresql/
