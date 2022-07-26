#!/bin/bash
tables=(ods_sdx_app ods_sdx_app_installed ods_sdx_dev ods_sdx_metadata)
for t in ${tables[@]};
do
hdfs dfs -du -h /user/hive/warehouse/ods_sdx_test.db/$t
hdfs dfs -rm -r /user/hive/warehouse/ods_sdx_test.db/$t/*
done

hdfs dfs -du -h .Trash/Current/user/hive/warehouse/ods_sdx_test.db
hdfs dfs -rm -r .Trash/Current/user/hive/warehouse/ods_sdx_test.db