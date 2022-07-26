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
