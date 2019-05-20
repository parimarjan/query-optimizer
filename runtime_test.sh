for i in {1..10}
do
  ./drop_cache.sh
  #./drop_cache_docker.sh
  sleep 1
  echo "run $i"
  START_TIME=$SECONDS
  psql -d imdb -U imdb < join-order-benchmark/17a.sql
  #psql -d imdb -U imdb -p 5400 < join-order-benchmark/20a.sql
  ELAPSED_TIME=$(($SECONDS - $START_TIME))
  echo "elapsed time: $ELAPSED_TIME"
done
