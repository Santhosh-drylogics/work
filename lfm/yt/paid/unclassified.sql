---- Fact Table ----
drop table if exists san_fact;
create table san_fact as select distinct youtube_video_dcs_uid, last_value(youtube_video_views) over(partition by youtube_video_dcs_uid order by fact_date_key asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as youtube_video_views  from fact_youtube_video_samples where youtube_video_dcs_uid IN (select youtube_video_dcs_uid from san_yt);

--- Organic table ---- 
drop table if exists san_yt_traffic_org; create table san_yt_traffic_org as select youtube_video_dcs_uid,sum(youtube_views) as youtube_views_org from fact_youtube_video_insight_traffic_source_samples2 where youtube_video_dcs_uid IN(select youtube_video_dcs_uid from san_yt) and youtube_traffic_source_type_name !='youtube_advertising' group by youtube_video_dcs_uid;
--- Paid Table ------
drop table if exists san_yt_traffic_paid; create table san_yt_traffic_paid as select youtube_video_dcs_uid,sum(youtube_views) as youtube_views_paid from fact_youtube_video_insight_traffic_source_samples2 where youtube_video_dcs_uid IN(select youtube_video_dcs_uid from san_yt) and youtube_traffic_source_type_name ='youtube_advertising' group by youtube_video_dcs_uid;

----accumulated table -----
drop table san_compare_yt; create table san_compare_yt as select distinct fct.youtube_video_dcs_uid, fct.youtube_video_views, tr.youtube_views_org, pd.youtube_views_paid from san_fact fct join san_yt_traffic_org tr on tr.youtube_video_dcs_uid=fct.youtube_video_dcs_uid join san_yt_traffic_paid pd on pd.youtube_video_dcs_uid=fct.youtube_video_dcs_uid;


----- YT Views -----
drop table san_yt_views;create table san_yt_views as select youtube_video_dcs_uid, greatest(youtube_video_views, addnull(youtube_views_org,youtube_views_paid)) as total_views, youtube_video_views, addNULL(youtube_views_org, youtube_views_paid) as total_insight_views,youtube_views_org as organic, youtube_views_paid as paid, (greatest(youtube_video_views, addnull(youtube_views_org,youtube_views_paid)) - youtube_views_org - youtube_views_paid) as unclassified from san_compare_yt;


Result:

Null values are not subtracted causing the issue