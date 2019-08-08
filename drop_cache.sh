#!/usr/bin/env bash
echo "drop cache!"
pg_ctl -D $PG_DATA_DIR -m i restart -l logfile
echo 1234 | sudo -S bash -c "echo 1 > /proc/sys/vm/drop_caches"
echo "drop cache done!"
#sudo systemctl restart postgresql
