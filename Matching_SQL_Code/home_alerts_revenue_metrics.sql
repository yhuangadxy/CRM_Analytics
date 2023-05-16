drop table if exists revenue_metrics;
create table revenue_metrics as
(
 select
    member_id,
    coalesce(sum(total_consumer_value),0) as total_consumer_value,
    coalesce(sum(total_referral_revenue),0) as total_referral_revenue,
    coalesce(sum(lead_sales_revenue),0) as lead_sales_revenue,
    coalesce(sum(media_revenue),0) as media_revenue,
    coalesce(sum(n_clicks),0) as media_clicks 
from (select adjusted_uu_id, total_consumer_value, total_referral_revenue, lead_sales_revenue, media_revenue, n_clicks
      from rdc_analytics.revenue.total_consumer_value_detail
      where event_date between '2022-02-24' and '2022-04-09'
     )  tcv
join (select adjusted_uu_id, member_id 
      from rdc_analytics.clickstream.unique_users_daily
      where 1=1
      and event_date between '2022-02-24' and '2022-04-09'
      and member_id is not null
     )  mem 
    on tcv.adjusted_uu_id = mem.adjusted_uu_id
 group by 1
);
