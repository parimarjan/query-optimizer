Installation and Setup
    - install postgres
        - TODO: add instructions
    - all calcite dependencies should be downloaded using pom.xml when building
    - all queries are in the repo
    - download imdb dataset
        - official instructions don't seem to work well, but following the
        steps here works:
        - https://github.com/RyanMarcus/imdb_pg_dataset/blob/master/vagrant/config.sh
    - start postgres server with imdb
        - init postgres, createdb etc.
        - postgres -D $DATA_DIR
        - settings should match in pg-schema.json

Running:
```bash
mvn package
mvn -e exec:java -Dexec.mainClass=Main
```

