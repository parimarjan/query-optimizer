#!/usr/bin/env bash
echo "drop cache!"
#sudo systemctl restart postgresql
pg_ctl restart -l logfile -D ~/pg_data_dir/
sudo bash -c "echo 1 > /proc/sys/vm/drop_caches"
echo "drop cache done!"
