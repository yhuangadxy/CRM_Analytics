drop table if exists registered_users_master;
create table registered_users_master as
(
 select member_id
 from rdc_analytics.crm.consumer_member_profile_current 

 union

 select member_id
 from rdc_analytics.crm.email_activity_details
 where event_date between '2022-02-24' and '2022-04-09'
 group by 1

 union

 select member_id
 from rdc_analytics.crm.notification_activity_details
 where event_date between '2022-02-24' and '2022-04-09'
 group by 1

 union

 select member_id
 from rdc_analytics.clickstream.unique_users_daily
 where event_date between '2022-02-24' and '2022-04-09'
 group by 1
);


/*
select count(*)
from registered_users_master

COUNT(*)
99,708,957
*/
