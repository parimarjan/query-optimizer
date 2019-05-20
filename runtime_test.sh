for i in {1..10}
do
  ./drop_cache.sh
  sleep 5
  echo "run $i"
  START_TIME=$SECONDS
  psql -d imdb < join-order-benchmark/20a.sql
  ELAPSED_TIME=$(($SECONDS - $START_TIME))
  echo "elapsed time: $ELAPSED_TIME"
done
