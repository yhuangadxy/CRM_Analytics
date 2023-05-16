drop table if exists member_data_consolidated;
create table member_data_consolidated as 
(
 select
    users.member_id,

    coalesce(email.home_alert_sent_email_count,0) as home_alert_sent_email_count,
    coalesce(email.other_txn_sent_email_count,0) as other_txn_sent_email_count,
    coalesce(email.promo_sent_email_count,0) as promo_sent_email_count,
    coalesce(email.home_alert_bounce_email_count,0) as home_alert_bounce_email_count,
    coalesce(email.other_txn_bounce_email_count,0) as  other_txn_bounce_email_count,
    coalesce(email.promo_bounce_email_count,0) as promo_bounce_email_count, 
    coalesce(email.home_alert_open_email_count,0) as home_alert_open_email_count,
    coalesce(email.other_txn_open_email_count,0) as other_txn_open_email_count,
    coalesce(email.promo_open_email_count,0) as promo_open_email_count,
    coalesce(email.home_alert_click_email_count,0) as home_alert_click_email_count,
    coalesce(email.other_txn_click_email_count,0) as other_txn_click_email_count,
    coalesce(email.promo_click_email_count,0) as promo_click_email_count,
    coalesce(email.home_alert_opt_in_email_count,0) as home_alert_opt_in_email_count,
    coalesce(email.other_txn_opt_in_email_count,0) as other_txn_opt_in_email_count,
    coalesce(email.promo_opt_in_email_count,0) as promo_opt_in_email_count,
    coalesce(email.home_alert_opt_out_email_count,0) as home_alert_opt_out_email_count,
    coalesce(email.other_txn_opt_out_email_count,0) as other_txn_opt_out_email_count,
    coalesce(email.promo_opt_out_email_count,0) as promo_opt_out_email_count,

    coalesce(push.home_alert_sent_push_count,0) as home_alert_sent_push_count,
    coalesce(push.other_sent_push_count,0) as other_sent_push_count,
    coalesce(push.home_alert_bounce_push_count,0) as home_alert_bounce_push_count,
    coalesce(push.other_bounce_push_count,0) as other_bounce_push_count,
    coalesce(push.home_alert_open_push_count,0) as home_alert_open_push_count,
    coalesce(push.other_open_push_count,0) as other_open_push_count,

    coalesce(web.visit_cnt,0) as visit_cnt,
    coalesce(web.visit_count_14day,0) as visit_count_14day,
    coalesce(web.minutes_per_vist,0) as minutes_per_vist,

    coalesce(web.saved_listing_cnt,0) as saved_listing_cnt,
    coalesce(web.saved_search_cnt,0) as saved_search_cnt,

    coalesce(web.total_for_sale_ldp,0) as total_for_sale_ldp,
    coalesce(web.total_new_construction_ldp,0) as total_new_construction_ldp,
    coalesce(web.total_rent_ldp,0) as total_rent_ldp,
    coalesce(web.total_owner_seller_ldp,0) as total_owner_seller_ldp,
    coalesce(web.total_sellermarketplace_ldp,0) as total_sellermarketplace_ldp,
    coalesce(web.total_myhome_ldp,0) as total_myhome_ldp,
    coalesce(web.total_sell_landing_ldp,0) as total_sell_landing_ldp,
    coalesce(web.total_pdp,0) as total_pdp,
    coalesce(web.total_rsp,0) as total_rsp,

    coalesce(web.total_for_sale_srp,0) as total_for_sale_srp,
    coalesce(web.total_new_construction_srp,0) as total_new_construction_srp,
    coalesce(web.total_rent_srp,0) as total_rent_srp,
    coalesce(web.total_owner_seller_srp,0) as total_owner_seller_srp,
    coalesce(web.total_sellermarketplace_srp,0) as total_sellermarketplace_srp,
    coalesce(web.total_myhome_srp,0) as total_myhome_srp,
    coalesce(web.total_sell_landing_srp,0) as total_sell_landing_srp,

    coalesce(prod_addl.fs_searches,0) as fs_searches,
    coalesce(prod_addl.fs_refined_searches_except_price_filter,0) as fs_refined_searches_except_price_filter,
    coalesce(prod_addl.fs_price_filter_searches,0) as fs_price_filter_searches,
    coalesce(prod_addl.fs_more_filter_clicks,0) as fs_more_filter_clicks,
    coalesce(prod_addl.fs_photo_gallery_clicks,0) as fs_photo_gallery_clicks,
    coalesce(prod_addl.fs_ldp_module_expands,0) as fs_ldp_module_expands,

    coalesce(prod_addl.fr_searches,0) as fr_searches,
    coalesce(prod_addl.fr_refined_searches_except_price_filter,0) as fr_refined_searches_except_price_filter,
    coalesce(prod_addl.fr_price_filter_searches,0) as fr_price_filter_searches,
    coalesce(prod_addl.fr_more_filter_clicks,0) as fr_more_filter_clicks,
    coalesce(prod_addl.fr_photo_gallery_clicks,0) as fr_photo_gallery_clicks,
    coalesce(prod_addl.fr_ldp_module_expands,0) as fr_ldp_module_expands,

    coalesce(leads.for_sale_leads_submitted,0) as for_sale_leads_submitted,
    coalesce(leads.for_rent_leads_submitted,0) as for_rent_leads_submitted,
    coalesce(leads.seller_leads_submitted,0) as seller_leads_submitted,
    coalesce(leads.desktop_leads_submitted,0) as desktop_leads_submitted,
    coalesce(leads.mweb_leads_submitted,0) as mweb_leads_submitted,
    coalesce(leads.ios_leads_submitted,0) as ios_leads_submitted,
    coalesce(leads.android_leads_submitted,0) as android_leads_submitted,
    coalesce(leads.for_sale_pure_market_leads_submitted,0) as for_sale_pure_market_leads_submitted,
    coalesce(leads.for_sale_choice_market_leads_submitted,0) as for_sale_choice_market_leads_submitted,

    coalesce(revenue.media_clicks,0) as media_clicks,
    coalesce(revenue.total_consumer_value,0) as total_consumer_value,
    coalesce(revenue.total_referral_revenue,0) as total_referral_revenue,
    coalesce(revenue.lead_sales_revenue,0) as lead_sales_revenue,
    coalesce(revenue.media_revenue,0) as media_revenue

 from registered_users_master  users
 left join email_events_pre_period  email
     on users.member_id = email.member_id
 left join notification_events_pre_preiod  push
   on users.member_id = push.member_id
 left join product_metrics  web
     on users.member_id = web.member_id
 left join clickstream_metrics_addtional  prod_addl
     on users.member_id = prod_addl.member_id
 left join lead_metrics  leads
     on users.member_id = leads.consumer_member_id_raw
 left join revenue_metrics  revenue
     on users.member_id = revenue.member_id
);


drop table if exists home_alerts_treatment;
create table home_alerts_treatment as 
(
select 
   data.*,
   1 as treatment
from member_data_consolidated  data
where 1=1
and (home_alert_sent_email_count + home_alert_open_email_count + home_alert_click_email_count + home_alert_bounce_email_count
         + home_alert_opt_in_email_count + home_alert_opt_out_email_count + home_alert_sent_push_count 
         + home_alert_bounce_push_count + home_alert_open_push_count) > 0
and (
     (fs_searches + fs_refined_searches_except_price_filter + fs_price_filter_searches + 
        fr_searches + fr_refined_searches_except_price_filter + fr_price_filter_searches) > 0 
     or
     (total_for_sale_ldp + total_rent_ldp + total_new_construction_ldp + total_owner_seller_ldp) > 1
    )
);


drop table if exists home_alerts_control;
create table home_alerts_control as 
(
select 
   data.*,
   0 as treatment
from member_data_consolidated  data
where 1=1
and (home_alert_sent_email_count + home_alert_open_email_count + home_alert_click_email_count + home_alert_bounce_email_count
         + home_alert_opt_in_email_count + home_alert_opt_out_email_count + home_alert_sent_push_count 
         + home_alert_bounce_push_count + home_alert_open_push_count) = 0
and (
     (fs_searches + fs_refined_searches_except_price_filter + fs_price_filter_searches + 
        fr_searches + fr_refined_searches_except_price_filter + fr_price_filter_searches) > 0 
     or
     (total_for_sale_ldp + total_rent_ldp + total_new_construction_ldp + total_owner_seller_ldp) > 1
    )
);


drop table if exists home_alerts_downsampled;
create table home_alerts_downsampled as 
(
 select *
 from home_alerts_treatment
 tablesample bernoulli (1) seed (99)
 
 union
 
 select *
 from home_alerts_control
 tablesample bernoulli (1) seed (99)
);
