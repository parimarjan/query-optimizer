echo "going to drop cache for docker!"
sudo docker restart docker-pg
sudo bash -c "echo 1 > /proc/sys/vm/drop_caches"
echo "dropped docker pg cache"
