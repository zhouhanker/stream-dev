/opt/soft/seatunnel-2.3.8/bin/seatunnel.sh -c /opt/soft/seatunnel-2.3.8/user_config/mysql_to_hbase.conf \
-i mysql.host='10.39.48.36' \
-m local

/opt/soft/seatunnel-2.3.8/bin/seatunnel.sh -c /opt/soft/seatunnel-2.3.8/user_config/mysql_to_hive_dim_area.conf \
-i mysql.host='10.39.48.36' \
-m local

/opt/soft/seatunnel-2.3.8/bin/seatunnel.sh -c /opt/soft/seatunnel-2.3.8/user_config/mysql_to_print.conf \
-i mysql.host='10.39.48.36' \
-m local