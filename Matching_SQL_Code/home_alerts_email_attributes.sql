drop table if exists home_alerts_email_campaign_ids_pre_period;
create table home_alerts_email_campaign_ids_pre_period as
(
 select 
     campaign_id
 from LEGACY_RAW.CNSM_RSPY_PDT.LAUNCH_STATE
 where 1=1
 --and year = '2022'
 --and month in ('02','03','04')
 --and concat(year,month,day) between '20220224' and '20220409'
 and lower(campaign_name) like '%alert%'
 and lower(campaign_name) not like '%test%'
 and launch_type in ('S','P','R')
 and launch_status = 'C'
 group by 1
);


/*
select count(*)
from home_alerts_email_campaign_ids_pre_period

COUNT(*)
105
*/


drop table if exists email_events_pre_period;
create table email_events_pre_period as 
(
 select 
    member_id,

    sum(case when action = 'sent' and campaign_id in  (select campaign_id from home_alerts_email_campaign_ids_pre_period) then 1 else 0 end) as home_alert_sent_email_count,
    sum(case when action = 'sent' and campaign_id not in  (select campaign_id from home_alerts_email_campaign_ids_pre_period) and purpose = 'Transactional' then 1 else 0 end) as other_txn_sent_email_count,
    sum(case when action = 'sent' and purpose = 'Promotional' then 1 else 0 end) as promo_sent_email_count,

    sum(case when action = 'bounce' and campaign_id in  (select campaign_id from home_alerts_email_campaign_ids_pre_period) then 1 else 0 end) as home_alert_bounce_email_count,
    sum(case when action = 'bounce' and campaign_id not in  (select campaign_id from home_alerts_email_campaign_ids_pre_period) and purpose = 'Transactional' then 1 else 0 end) as other_txn_bounce_email_count,
    sum(case when action = 'bounce' and purpose = 'Promotional' then 1 else 0 end) as promo_bounce_email_count,

    sum(case when action = 'open' and campaign_id in  (select campaign_id from home_alerts_email_campaign_ids_pre_period) then 1 else 0 end) as home_alert_open_email_count,
    sum(case when action = 'open' and campaign_id not in  (select campaign_id from home_alerts_email_campaign_ids_pre_period) and purpose = 'Transactional' then 1 else 0 end) as other_txn_open_email_count,
    sum(case when action = 'open' and purpose = 'Promotional' then 1 else 0 end) as promo_open_email_count,

    sum(case when action = 'click' and campaign_id in  (select campaign_id from home_alerts_email_campaign_ids_pre_period) then 1 else 0 end) as home_alert_click_email_count,
    sum(case when action = 'click' and campaign_id not in  (select campaign_id from home_alerts_email_campaign_ids_pre_period) and purpose = 'Transactional' then 1 else 0 end) as other_txn_click_email_count,
    sum(case when action = 'click' and purpose = 'Promotional' then 1 else 0 end) as promo_click_email_count,

    sum(case when action = 'opt_in' and campaign_id in  (select campaign_id from home_alerts_email_campaign_ids_pre_period) then 1 else 0 end) as home_alert_opt_in_email_count,
    sum(case when action = 'opt_in' and campaign_id not in  (select campaign_id from home_alerts_email_campaign_ids_pre_period) and purpose = 'Transactional' then 1 else 0 end) as other_txn_opt_in_email_count,
    sum(case when action = 'opt_in' and purpose = 'Promotional' then 1 else 0 end) as promo_opt_in_email_count,

    sum(case when action = 'opt_out' and campaign_id in  (select campaign_id from home_alerts_email_campaign_ids_pre_period) then 1 else 0 end) as home_alert_opt_out_email_count,
    sum(case when action = 'opt_out' and campaign_id not in  (select campaign_id from home_alerts_email_campaign_ids_pre_period) and purpose = 'Transactional' then 1 else 0 end) as other_txn_opt_out_email_count,
    sum(case when action = 'opt_out' and purpose = 'Promotional' then 1 else 0 end) as promo_opt_out_email_count

 from rdc_analytics.crm.email_activity_details
 where event_date between '2022-02-24' and '2022-04-09'
 group by 1
);
