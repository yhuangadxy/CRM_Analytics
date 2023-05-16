drop table if exists notification_events_pre_preiod;
create table notification_events_pre_preiod as
(
 select
    member_id,
    platform as notification_platform,

    sum(case when action = 'send' and campaign_id in ('3d426d53-9dcf-48d1-8273-bb83c2a25876', 'de9d7ccf-b427-4d62-a765-129de45f0d90') then 1 else 0 end) as home_alert_sent_push_count,
    sum(case when action = 'send' and campaign_id not in ('3d426d53-9dcf-48d1-8273-bb83c2a25876', 'de9d7ccf-b427-4d62-a765-129de45f0d90') then 1 else 0 end) as other_sent_push_count,

    sum(case when action = 'bounce' and campaign_id in ('3d426d53-9dcf-48d1-8273-bb83c2a25876', 'de9d7ccf-b427-4d62-a765-129de45f0d90') then 1 else 0 end) as home_alert_bounce_push_count,
    sum(case when action = 'bounce' and campaign_id not in ('3d426d53-9dcf-48d1-8273-bb83c2a25876', 'de9d7ccf-b427-4d62-a765-129de45f0d90') then 1 else 0 end) as other_bounce_push_count,

    sum(case when action = 'open' and campaign_id in ('3d426d53-9dcf-48d1-8273-bb83c2a25876', 'de9d7ccf-b427-4d62-a765-129de45f0d90') then 1 else 0 end) as home_alert_open_push_count,
    sum(case when action = 'open' and campaign_id not in ('3d426d53-9dcf-48d1-8273-bb83c2a25876', 'de9d7ccf-b427-4d62-a765-129de45f0d90') then 1 else 0 end) as other_open_push_count
 
 from rdc_analytics.crm.notification_activity_details
 where event_date between '2022-02-24' and '2022-04-09'
 group by 1,2
);
