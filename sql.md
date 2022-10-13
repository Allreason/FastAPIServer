select sn,buildversion from dwd_sdx_safe.dwd_sdx_dev_zip
where  subregion=date_sub(current_date(),0)
and subregion=1024
# new_in_list
create table if not exists ads_sdx_safe.snlist(sn string);LOAD DATA LOCAL INPATH '/home/hduser8006/snlist.csv' OVERWRITE INTO TABLE ads_sdx_safe.snlist;
set hive.cli.print.header=true;
select dev.tenantcode,sns.*,dev.buildversion as buildversion, dev.sendtimestamp as last_time_report,
case when dev2.sn is NULL then 'offline' else 'online' end as isonline
from ads_sdx_safe.snlist as sns
left join
(
select tenantcode,sn,buildversion, sendtimestamp from dwd_sdx_safe.dwd_sdx_dev_zip
where subregion=date_sub(current_date(),0)
)dev
on dev.sn=sns.sn
left join
(
select distinct tenantcode,sn from ods_sdx_safe.ods_sdx_dev
where subregion=date_sub(current_date(),0)
and unix_timestamp(sendtimestamp)>=unix_timestamp(current_timestamp)-7200
) dev2
on dev.sn=dev2.sn and dev.tenantcode=dev2.tenantcode;

# 2022-10-09
select b.sn from (select sn from (select sn from ods_sdx_safe.ods_sdx_dev_zip where subregion > '2022-07-10'  and tenantcode = '1004' and sendtimestamp < date_sub(subregion, 10)) a group by sn) b inner join ods_sdx_safe.sn c on b.sn = c.sn;

# before-2022-09-29T21:40:47.907251
select b.sn from (select sn from ods_sdx_safe.ods_sdx_dev_zip where subregion = current_date() and tenantcode = '1004') a inner join ods_sdx_safe.sn b on a.sn = b.sn;

# before-2022-09-29T21:32:32.511321
select sn from (select sn from ods_sdx_safe.ods_sdx_dev_zip where subregion > '2022-07-10' and subregion < '2022-09-21' and tenantcode = '1004' and sendtimestamp < date_sub(subregion, 10) and sendtimestamp > '2022-07-01') a group by sn;
# before-2022-09-29T10:15:11.180812
select sn from (select sn from ods_sdx_safe.ods_sdx_dev_zip where subregion > '2022-07-10' and subregion < '2022-09-21' and tenantcode = '1004' and sendtimestamp < date_sub(subregion, 10)) a group by sn;
# before-2022-09-29T09:31:00.724419
select sn from (select sn from ods_sdx_safe.ods_sdx_dev_zip where subregion > '2022-07-10' and  subregion < '2022-09-21'  and tenantcode = '1004' and sendtimestamp < date_sub(subregion, 10)) group by sn;
# before-2022-09-29T09:29:59.491106
select sn from (select sn from ods_sdx_safe.ods_sdx_dev_zip where subregion > '2022-07-10' and  subregion < '2022-09-21' and sendtimestamp < date_sub(subregion, 10)) group by sn;
# before-2022-09-29T09:28:02.708651
select sn from ods_sdx_safe.ods_sdx_dev_zip where subregion=date_sub(current_date(),0) and tenantcode = '1024' and buildversion = '3663';
# before-2022-09-29T08:41:53.911965
select sn from ods_sdx_safe.ods_sdx_dev_zip where subregion=date_sub(current_date(),0) and tenantcode = '1004' and buildversion = '5795';
# before-2022-09-29T08:41:06.100518
select sn from dwd_sdx_safe.dwd_sdx_dev_zip where subregion=date_sub(current_date(),0) and tenantcode = '1004' and buildversion = '5795';

# before-2022-09-28T21:48:56.160556
select sn from dwd_sdx_safe.dwd_sdx_dev_zip
where subregion=date_sub(current_date(),0) and tenantcode = '1004' and buildversion = '5795';

# before-2022-09-28T21:47:33.602429
select sn from dwd_sdx_safe.dwd_sdx_dev_zip
where subregion=date_sub(current_date(),0) where tenantcode = '1004' and buildversion = '5795';
# before-2022-09-28T21:46:52.666303
select a.sn, b.gid from ads_sdx_safe.needed_sn6 a left join (
select device_id,collect_list(groupname) as gid
from
(
select gg0.*,gg1.group_name as groupname from
(
select device_id,group_id  from
ods_sdx_safe.ods_sdx_device_group_map where tenantcode = '1034' and group_id!='0'
) gg0
left join
(
select group_name,group_id  from ods_sdx_safe.gz_iot_device_group where tenantcode = '1034'
) gg1
on gg0.group_id=gg1.group_id
) ggg
group by device_id) b on a.sn = b.device_id;
# before-2022-09-21T11:45:46.013974
select devall.*,installed1.packagename,installed2.packagename,case when dev.sn is NULL then 'offline' else 'online' end
from
(
select tenantcode,sn,buildversion
from dwd_sdx_safe.dwd_sdx_dev_zip
where subregion=date_sub(current_date(),0)
and tenantcode in ('1010','1015','1024','1028','1034','1036')
)devall
left join
(
select sn,packagename,versioncode,versionname from ods_sdx_safe.ods_sdx_app_installed_zip
where subregion=date_sub(current_date(),0)
and tenantcode in ('1010','1015','1024','1028','1034','1036')
and packagename='news.androidtv.launchonboot'
) installed1
on devall.sn=installed1.sn
left join
(
select sn,packagename,versioncode,versionname from ods_sdx_safe.ods_sdx_app_installed_zip
where subregion=date_sub(current_date(),0)
and tenantcode in ('1010','1015','1024','1028','1034','1036')
and packagename='flar2.homebutton'
) installed2
on devall.sn=installed2.sn
left join
(
select distinct sn from ods_sdx_safe.ods_sdx_dev
where subregion=date_sub(current_date(),0)
and unix_timestamp(sendtimestamp)>=unix_timestamp(current_timestamp)-7200
) dev
on dev.sn=devall.sn
where installed1.packagename is not null or installed2.packagename is not null
# before-2022-09-19
select skywayver, count(*) from dwd_sdx_safe.dwd_sdx_metadata_zip
where subregion=date_sub(current_date(),0) group by skywayver;
# before-2022-09-16T09:08:20.684909
select tenantcode,sn,skywayver from dwd_sdx_safe.dwd_sdx_metadata_zip
where subregion=date_sub(current_date(),0)

# activated_in_list
create table if not exists ads_sdx_safe.snlist(sn string);LOAD DATA LOCAL INPATH '/home/hduser8006/snlist.csv' OVERWRITE INTO TABLE ads_sdx_safe.snlist;
set hive.cli.print.header=true;
select sns.*,dev.buildversion as buildversion, dev.sendtimestamp as last_time_report,meta.sendtimestamp as activated_time,
case when dev2.sn is NULL then 'offline' else 'online' end as isonline
from ads_sdx_safe.snlist as sns
left join
(
select sn,buildversion, sendtimestamp from ods_sdx_safe.ods_sdx_dev_zip
where subregion=date_sub(current_date(),0)
)dev
on dev.sn=sns.sn
left join
(
select sn,sendtimestamp from
ods_sdx_safe.ods_sdx_dev_activated_zip
where subregion=date_sub(current_date(),0)
)meta
on meta.sn=sns.sn
left join
(
select distinct sn from ods_sdx_safe.ods_sdx_dev
where subregion=date_sub(current_date(),0)
and unix_timestamp(sendtimestamp)>=unix_timestamp(current_timestamp)-7200
) dev2
on dev.sn=dev2.sn;
# before-2022-09-14T18:24:06.518419
select dev.*
from
(
select distinct sn from ods_sdx_safe.ods_sdx_dev
where subregion=date_sub(current_date(),1)
and tenantcode=1015
and sendtimestamp>='2022-09-10 20:50:00'
and sendtimestamp<='2022-09-10 22:10:00'
)dev
left join
(
select sn from dwd_sdx_safe.dwd_sdx_app_installed_zip
where subregion=date_sub(current_date(),0)
and tenantcode=1015
and packagename='com.nes.coreservice'
and versionname='1.0.0.25'
)ins
on dev.sn=ins.sn
where ins.sn is null
# 2022-09-11
select snlist.*,dev.tenantcode from
(
select * from
ads_sdx_safe.snlist2022_09_08
) snlist
left join
(
    select tenantcode,device_id from ods_sdx_safe.ods_sdx_device
) dev
on dev.device_id=snlist.sn

# old
null
# 2022-09-08
set hive.cli.print.header=true;
select installed.sn from
(
select * from ods_sdx_safe.gz_iot_device_group
where group_name='New Production'
and tenantcode=1015
) groupname
inner join
(
    select * from
    ods_sdx_safe.ods_sdx_device_group_map
    where tenantcode=1015
)as groupmap
on groupname.deployid=groupmap.deployid
and groupname.tenantcode=groupmap.tenantcode
and groupname.group_id=groupmap.group_id
inner join
(
select deployid,tenantcode,sn,packagename,versioncode,versionname from dwd_sdx_safe.dwd_sdx_app_installed_zip
where subregion=date_sub(current_date(),0)
and tenantcode=1015
and packagename='com.hbo.hbonow'
) as installed
on installed.deployid=groupmap.deployid
and installed.tenantcode=groupmap.tenantcode
and installed.sn=groupmap.device_id
# 2022-09-02
set hive.cli.print.header=true;
select installed.*,metadata.androidver,case when groupmap.group_id='79a226aa0d6f11ed89f90a4b91deffde' then 'OFT CoreServices' else 'TFT VTR' end
from
(
select deployid,tenantcode,sn,packagename,versioncode,versionname from dwd_sdx_safe.dwd_sdx_app_installed_zip
where subregion=date_sub(current_date(),1)
and tenantcode=1015
and packagename='com.nes.coreservice'
) as installed
inner join
(
select deployid,tenantcode,sn,androidver from dwd_sdx_safe.dwd_sdx_metadata_zip
where subregion=date_sub(current_date(),1)
and tenantcode=1015
) as metadata
on metadata.sn=installed.sn
inner join
(
    select * from
    ods_sdx_safe.ods_sdx_device_group_map
    where group_id in ('79a226aa0d6f11ed89f90a4b91deffde','6256e9673a6711eb9cf60242ac120002')
)as groupmap
on installed.sn=groupmap.device_id
and installed.deployid=groupmap.deployid
and installed.tenantcode=groupmap.tenantcode
# ooo
set hive.cli.print.header=true;
select installed.*,case when groupmap.group_id='79a226aa0d6f11ed89f90a4b91deffde' then 'OFT CoreServices' else 'TFT VTR' end
from
(
select deployid,tenantcode,sn,androidver,apkver,skywayver from dwd_sdx_safe.dwd_sdx_metadata_zip
where subregion=current_date()
and tenantcode=1015
) as installed
inner join
(
    select * from
    ods_sdx_safe.ods_sdx_device_group_map
    where group_id in ('79a226aa0d6f11ed89f90a4b91deffde','6256e9673a6711eb9cf60242ac120002')
)as groupmap
on installed.sn=groupmap.device_id
and installed.deployid=groupmap.deployid
and installed.tenantcode=groupmap.tenantcode
# before-2022-08-10T10:48:04.908515

# before 2022-08-09T14:05:21.966983
set hive.cli.print.header=true;
select a.sn, b.buildversion
from ods_sdx_safe.tmp_sn2 a
left join ods_sdx_safe.ods_sdx_dev_zip b
on b.subregion = current_date()
and b.tenantcode = '1027'
and a.sn =  b.sn;
# j
select sn,totalmemory-(applicationoccurpymemory+otheroccurpymemory+systemoccurpymemory) as remaining_memory_space
from dwd_sdx_safe.dwd_sdx_dev_zip
where subregion=date_sub(current_date(),0)
and tenantcode in ('1010')
and totalmemory-(applicationoccurpymemory+otheroccurpymemory+systemoccurpymemory)<=800
# 2022-08-05-2
select sn,packagename,versioncode,sendtimestamp
from ods_sdx_safe.ods_sdx_app_installed_zip
where subregion=date_sub(current_date(),0)
and packagename='com.nes.skywayclient'
and tenantcode=1027
and versioncode='2022042811';

# 2022-08-05
select sn,packagename,versioncode,sendtimestamp
from ods_sdx_safe.ods_sdx_app_installed_zip
where subregion=date_sub(current_date(),0)
and packagename='com.nes.coreservice'
and tenantcode=1027
and versioncode='202206142';
# 2022-07-27
select count(*),versionname
from ods_sdx_safe.ods_sdx_app_installed_zip
where subregion=date_sub(current_date(),0)
and packagename='de.exaring.waipu'
and tenantcode=1018
group by versionname
# 2022-07-26
set hive.cli.print.header=true;
select me.*,bui.buildversion,case when dev.sn is NULL then 'offline' else 'online' end
from
(
select * from ads_sdx_safe.needed_sn5
) me
left join
(
select sn,buildversion
from dwd_sdx_safe.dwd_sdx_dev_zip
where subregion=date_sub(current_date(),0)
) bui
on me.sn=bui.sn
left join
(
select distinct sn from ods_sdx_safe.ods_sdx_dev
where subregion=date_sub(current_date(),0)
and unix_timestamp(sendtimestamp)>=unix_timestamp(current_timestamp)-7200
) dev
on me.sn=dev.sn;
# nnn
set hive.cli.print.header=true;
select tenantcode,count(*)
from
(
select tenantcode,sn from
dwd_sdx_safe.dwd_sdx_dev
where subregion>=date_sub(current_date(),30)
and active=true
group by tenantcode,sn
) a
group by tenantcode;
# 2022-07-20
set hive.cli.print.header=true;
select dev1.*,installed.versioncode as skywayclient_version from
(
select sn,sendtimestamp as last_time_online
from dwd_sdx_safe.dwd_sdx_dev_zip
where subregion=date_sub(current_date(),0)
and tenantcode in (1034)
) dev1
left join
(
select sn,versioncode
from ods_sdx_safe.ods_sdx_app_installed_zip
where subregion=date_sub(current_date(),0)
and packagename="com.nes.skywayclient"
) installed
on dev1.sn=installed.sn;
# another heading
# skyway prod
select sn,versioncode
from ods_sdx_safe.ods_sdx_app_installed_zip
where subregion=date_sub(current_date(),1)
and packagename='com.nes.skywayclient'
limit 100;