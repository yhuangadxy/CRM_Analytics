drop table if exists lead_metrics;
create table lead_metrics as
(
 select
     consumer_member_id_raw,
     sum(case when lead_vertical = 'for_sale' then 1 else 0 end) as for_sale_leads_submitted,
     sum(case when lead_vertical = 'for_rent' then 1 else 0 end) as for_rent_leads_submitted,
     sum(case when lead_vertical = 'Seller' then 1 else 0 end) as seller_leads_submitted,
     sum(case when platform = 'Desktop' then 1 else 0 end) as desktop_leads_submitted,
     sum(case when platform = 'Mobile Web' then 1 else 0 end) as mweb_leads_submitted,
     sum(case when platform = 'iOS App' then 1 else 0 end) as ios_leads_submitted,
     sum(case when platform = 'Android App' then 1 else 0 end) as android_leads_submitted,
     sum(case when lead_vertical = 'for_sale' and market_type_at_submission = 'pure' then 1 else 0 end) as for_sale_pure_market_leads_submitted,
     sum(case when lead_vertical = 'for_sale' and market_type_at_submission = 'choice' then 1 else 0 end) as for_sale_choice_market_leads_submitted
 from RDC_ANALYTICS.LEADS.SUBMITTED_LEAD_DETAIL
 where 1=1
 and consumer_member_id_raw is not null
 and submitted_lead_date between '2022-02-24' and '2022-04-09'
 group by 1
);
