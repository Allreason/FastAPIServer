jfk
# before 2022-08-09T14:12:01.871425
select * from
ods_sdx_safe.ods_sdx_metadata
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
