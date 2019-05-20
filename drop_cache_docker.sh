echo "drop cache!"
#sudo systemctl restart postgresql
sudo docker restart docker-pg
sudo bash -c "echo 1 > /proc/sys/vm/drop_caches"
