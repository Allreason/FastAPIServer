# edmprod-conf
#evo-platform-prod
mysql_host_prod=edm-pro-skyway-mysql.chlsgrutzvog.us-east-2.rds.amazonaws.com
mysql_port_prod=3306
mysql_username_prod=skyway
mysql_password_prod=n4OwDVtM
mysql_host_prod_migrate=172.31.22.91

MONGODB_CLUSTER_HOST_PROD=172.31.15.231:27017,172.31.2.88:27017,172.31.2.88:27018
MONGODB_USER_PROD=root
MONGODB_PASSWORD_PROD=sei!2#
MONGODB_CONNECTTIMEOUTMS=500000
MONGODB_SAMPLE_POOL_SIZE=20
MONGODB_SOCKET_TIMEOUT_MS=60000
MONGODB_MAX_IDLE_TIME_MS=0
MONGODB_KEEP_ALIVE_MS=5000

#evo-platform-test
mysql_host_test=172.31.33.141
mysql_port_test=3306
mysql_username_test=skyway
mysql_password_test=n4OwDVtM

MONGODB_CLUSTER_HOST_TEST=172.31.33.141:27017
MONGODB_USER_TEST=root
MONGODB_PASSWORD_TEST=e9Fs3LDJ

MONGODB_COLLECTION=deviceAppInstallation
HIVE_TABEL_FROM_MONGO=ods_sdx_safe.ods_sdx_app_installed_mongo

# edmprod-metadata
#!/bin/bash
source conf/common.properties
ods_table=ods_sdx_metadata
fields=`mysql -usage -h$mysql_host_prod_migrate -p'Abc123!@#' sage_task_metadata -N -e "SELECT fieldnames  from sdx_kafka_hive_eventname where tb = '"${ods_table}"' limit 1"` || { echo "query fields error "; exit 1;}
fields=${fields//,subregion/}
bfields="b."${fields//,/,b.}
bfields=${bfields//b.agentid/case when b.agentid  <>  \'\' and b.agentid is not null  then  b.agentid else '1' end }
echo "start"
date=`date +%Y-%m-%d`
export path2=$PWD
echo $path2
cd jar/
spark-submit \
--queue root.users \
--master yarn \
--driver-memory 3G \
--executor-memory 4G \
--executor-cores 2 \
--num-executors 10 \
--conf spark.executor.memoryOverhead=2048 \
--conf spark.streaming.backpressure.enabled=true \
--conf "spark.sql.hive.metastore.jars=/opt/cloudera/parcels/CDH/jars/*" \
--conf spark.sql.hive.metastore.version=2.1.1 \
--conf spark.shuffle.file.buffer=128 \
--conf spark.reducer.maxSizeInFlight=96 \
--class org.sei.main.SparkSQLEngine \
--jars seiemr-1.0-SNAPSHOT-jar-with-dependencies.jar  \
seiemr-1.0-SNAPSHOT.jar "set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 80000000;
set hive.merge.smallfiles.avgsize=32000000;
INSERT overwrite TABLE ods_sdx_safe.ods_sdx_metadata_zip partition(subregion = '"${date}"')
SELECT ${fields}
FROM
  (SELECT *,
          row_number()over(PARTITION BY deployid, tenantcode, sn
                           ORDER BY sendtimestamp DESC) row_num
   FROM (select ${fields} from ods_sdx_safe.ods_sdx_metadata_zip where subregion = date_sub(current_date, 1) union all select ${fields} from ods_sdx_safe.ods_sdx_metadata where subregion >= date_sub(current_date, 1) and sendtimestamp < current_date)d)e
   WHERE row_num = 1;
insert overwrite table dwd_sdx_safe.dwd_sdx_metadata_zip partition (subregion = '"${date}"') 
select ${bfields} from ods_sdx_safe.ods_sdx_device a 
inner join (select * from ods_sdx_safe.ods_sdx_metadata_zip where subregion = '"${date}"')b 
on a.device_id = b.sn and a.tenantcode = b.tenantcode and a.deployid = b.deployid ;"
   
   
# edmprod-dev
#!/bin/bash
source conf/common.properties
ods_table=ods_sdx_dev
fields=`mysql -usage -h$mysql_host_prod_migrate -p'Abc123!@#'  sage_task_metadata -N -e "SELECT fieldnames  from sdx_kafka_hive_eventname where tb = '"${ods_table}"' limit 1"` || { echo "query fields error "; exit 1;}
fields=${fields//,subregion/}
bfields="b."${fields//,/,b.}
bfields=${bfields//b.agentid/case when b.agentid  <>  \'\' and b.agentid is not null  then  b.agentid else '1' end }
echo "start"
date=`date +%Y-%m-%d`
lastdate=`mysql -uhive -h$mysql_host_prod_migrate -p'Abc123!@#'  hive -N -e "SELECT substr(MAX(PARTITIONS.PART_NAME), 11) FROM
DBS
INNER JOIN
TBLS ON DBS.DB_ID = TBLS.DB_ID
INNER JOIN
PARTITIONS ON TBLS.TBL_ID = PARTITIONS.TBL_ID and
DBS.NAME = 'ods_sdx_safe' and TBLS.TBL_NAME = 'ods_sdx_dev_zip';"`
export path2=$PWD
echo $path2
cd jar/
spark-submit \
--queue root.users \
--master yarn \
--driver-memory 3G \
--executor-memory 4G \
--executor-cores 2 \
--num-executors 10 \
--conf spark.executor.memoryOverhead=2048 \
--conf spark.streaming.backpressure.enabled=true \
--conf "spark.sql.hive.metastore.jars=/opt/cloudera/parcels/CDH/jars/*" \
--conf spark.sql.hive.metastore.version=2.1.1 \
--conf spark.shuffle.file.buffer=128 \
--conf spark.reducer.maxSizeInFlight=96 \
--class org.sei.main.SparkSQLEngine \
--jars seiemr-1.0-SNAPSHOT-jar-with-dependencies.jar  \
seiemr-1.0-SNAPSHOT.jar "set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 80000000;
set hive.merge.smallfiles.avgsize=32000000;
INSERT overwrite TABLE ods_sdx_safe.ods_sdx_dev_zip partition(subregion = '"${date}"')
SELECT ${fields}
FROM
  (SELECT *,
          row_number()over(PARTITION BY deployid, tenantcode, sn
                           ORDER BY sendtimestamp DESC) row_num
   FROM (select ${fields} from ods_sdx_safe.ods_sdx_dev_zip where  subregion = '"${lastdate}"' union all select ${fields} from ods_sdx_safe.ods_sdx_dev where subregion >= '"${lastdate}"' )d)e
   WHERE row_num = 1;
insert overwrite table dwd_sdx_safe.dwd_sdx_dev_zip partition (subregion = '"${date}"') 
select ${bfields} from ods_sdx_safe.ods_sdx_device a 
inner join (select * from ods_sdx_safe.ods_sdx_dev_zip where subregion = '"${date}"')b 
on a.device_id = b.sn and a.tenantcode = b.tenantcode and a.deployid = b.deployid;"

# edmprod-installed
#!/bin/bash
date=`date +%Y-%m-%d`
cd jar/
spark-submit \
--queue root.users \
--master yarn \
--driver-memory 3G \
--executor-memory 4G \
--executor-cores 2 \
--num-executors 10 \
--conf spark.executor.memoryOverhead=2048 \
--conf spark.streaming.backpressure.enabled=true \
--conf "spark.sql.hive.metastore.jars=/opt/cloudera/parcels/CDH/jars/*" \
--conf spark.sql.hive.metastore.version=2.1.1 \
--conf spark.shuffle.file.buffer=128 \
--conf spark.reducer.maxSizeInFlight=96 \
--class org.sei.main.SparkSQLEngine \
--jars seiemr-1.0-SNAPSHOT-jar-with-dependencies.jar  \
seiemr-1.0-SNAPSHOT.jar "set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite TABLE ods_sdx_safe.ods_sdx_app_installed_zip partition(subregion = '${date}')
SELECT deployid,sn,sendtimestamp,tenantcode,issystemapp,storageused,
appname,packagename,versionname,enabled,versioncode,customertype,
customername,eventname,stbusername,agentid,localcreatetime
FROM
(SELECT *,row_number()over(PARTITION BY deployid, tenantcode, sn,packagename
ORDER BY sendtimestamp DESC) row_num
FROM (select * from ods_sdx_safe.ods_sdx_app_installed_zip where subregion=date_sub(current_date, 1) 
union all select * from ods_sdx_safe.ods_sdx_app_installed where subregion>=date_sub(current_date, 1) )d)e
WHERE row_num = 1;"