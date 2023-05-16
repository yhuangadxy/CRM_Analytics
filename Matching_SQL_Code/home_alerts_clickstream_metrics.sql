drop table if exists product_metrics;
create table product_metrics as
(
 select
    member_id,
    --left(consumer_ip_zip, 2) as ip_zip_first_two_digits,


    sum(total_visits) as visit_cnt,
    sum(case when abs(datediff(day,event_date,'2022-04-09')) <=14 then total_visits else 0 end) as visit_count_14day,
    round(sum(visit_duration)/sum(total_visits)/60,2) as minutes_per_vist,

    sum(total_saved_listing) as saved_listing_cnt,
    sum(total_saved_search) as saved_search_cnt,

    sum(total_for_sale_ldp) as total_for_sale_ldp,
    sum(total_new_construction_ldp) as total_new_construction_ldp,
    sum(total_rent_ldp) as total_rent_ldp,
    sum(total_owner_seller_ldp) as total_owner_seller_ldp,
    sum(total_sellermarketplace_ldp) as total_sellermarketplace_ldp,
    sum(total_myhome_ldp) as total_myhome_ldp,
    sum(total_sell_landing_ldp) as total_sell_landing_ldp,
    sum(TOTAL_OFFMARKET_NOT_FOR_SALE_LDP) as total_pdp,
    sum(TOTAL_RECENTLYSOLD_NOT_FOR_SALE_LDP) as total_rsp,

    sum(total_for_sale_srp) as total_for_sale_srp,
    sum(total_new_construction_srp) as total_new_construction_srp,
    sum(total_rent_srp) as total_rent_srp,
    sum(total_owner_seller_srp) as total_owner_seller_srp,
    sum(total_sellermarketplace_srp) as total_sellermarketplace_srp,
    sum(total_myhome_srp) as total_myhome_srp,
    sum(total_sell_landing_srp) as total_sell_landing_srp

 from rdc_analytics.clickstream.unique_users_daily
 where 1=1
 and event_date between '2022-02-24' and '2022-04-09'
 group by 1
);



drop table if exists clickstream_metrics_addtional;
create table clickstream_metrics_addtional as 
(
 select
    member_id,

    sum(case when site_section = 'for_sale' and lower(event_name) = 'search' then 1 else 0 end) as fs_searches,
    sum(case when site_section = 'for_sale' and lower(event_name) = 'refinedsearch' and (search_filter_one not like '%price%' and search_filter_two not like '%price%') then 1 else 0 end) as fs_refined_searches_except_price_filter,
    sum(case when site_section = 'for_sale' and lower(event_name) like '%search%' and (search_filter_one like '%price%' or search_filter_two like '%price%') then 1 else 0 end) fs_price_filter_searches,
    sum(case when site_section = 'for_sale' and lower(event_name) = 'click' and click_activity_web like any ('%more_filters%', '%more-filters%') then 1 else 0 end) as fs_more_filter_clicks,
    sum(case when site_section = 'for_sale' and click_from_page_name like '%ldp%' and lower(event_name) = 'click' and click_activity_web like any ('hero:main_image','hero:photos','%gallery%') then 1 else 0 end) as fs_photo_gallery_clicks,
    sum(case when site_section = 'for_sale' and lower(event_name) = 'click' and click_activity_web like '%expand' then 1 else 0 end) as fs_ldp_module_expands,

    sum(case when site_section = 'for_rent' and lower(event_name) = 'search' then 1 else 0 end) as fr_searches,
    sum(case when site_section = 'for_rent' and lower(event_name) = 'refinedsearch' and (search_filter_one not like '%price%' and search_filter_two not like '%price%') then 1 else 0 end) as fr_refined_searches_except_price_filter,
    sum(case when site_section = 'for_rent' and lower(event_name) like '%search%' and (search_filter_one like '%price%' or search_filter_two like '%price%') then 1 else 0 end) fr_price_filter_searches,
    sum(case when site_section = 'for_rent' and lower(event_name) = 'click' and click_activity_web like any ('%more_filters%', '%more-filters%') then 1 else 0 end) as fr_more_filter_clicks,
    sum(case when site_section = 'for_rent' and click_from_page_name like '%ldp%' and lower(event_name) = 'click' and click_activity_web like any ('hero:main_image','hero:photos','%gallery%') then 1 else 0 end) as fr_photo_gallery_clicks,
    sum(case when site_section = 'for_rent' and lower(event_name) = 'click' and click_activity_web like '%expand' then 1 else 0 end) as fr_ldp_module_expands


 from legacy_bridge.biz_data_product_event_v2.rdc_biz_data
 where 1=1
 and event_date between '2022-02-24' and '2022-04-09'
 and member_id is not null
 group by 1
);
