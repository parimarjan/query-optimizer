#sudo docker restart docker-pg
echo "drop cache!"
sudo systemctl restart postgresql
sudo bash -c "echo 1 > /proc/sys/vm/drop_caches"
