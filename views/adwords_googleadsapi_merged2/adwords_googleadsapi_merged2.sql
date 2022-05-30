select 
  isnull(old_data.date_stop, new_data.date_stop) as date_stop,
  isnull(old_data.campaign_id, new_data.campaign_id) as campaign_id,
  isnull(old_data.name, new_data.name) as name_merged,
  old_data.name as name_old,
  new_data.name as name_new,
  old_data.total_cost as total_cost_old,
  new_data.total_cost as total_cost_new,
  old_data.total_clicks as total_clicks_old,
  new_data.total_clicks as total_clicks_new,
  new_data.total_interactions as total_interactions_new,
  old_data.total_impressions as total_impressions_old,
  new_data.total_impressions as total_impressions_new
from
  -- Old Data Source (Adwords)
  (select
      date(date_stop) as date_stop,
      campaign_names.name,  
      cost_data.campaign_id, 
      sum(cost) / 1000000.0 as total_cost,
      sum(clicks) as total_clicks,
      sum(impressions) as total_impressions
  from(
      (select 
        campaign_id, 
        date_stop, 
        received_at, 
        cost, clicks, 
        impressions, 
        row_number() OVER (PARTITION by campaign_id, date_stop ORDER BY received_at DESC) as rn 
      from adwords.campaign_performance_reports
      where true
        and date_stop >= '2020-01-01'
      ) cost_data
      inner join(
        select distinct 
          id, 
          name, 
          split_part(name, '_|_', 8) as product
        from adwords.campaigns 
      ) campaign_names on cost_data.campaign_id = campaign_names.id)
  where true
      and rn = 1
  group by 1,2,3) old_data

full join 
 
  -- New Data Source (googleadsapi)
  (select 
      date(segments__date) as date_stop,
      campaign_names.name,  
      cost_data.campaign__id as campaign_id,   
      sum(metrics__cost_micros) / 1000000.0 as total_cost,
      sum(metrics__interactions) as total_interactions,
      sum(metrics__clicks) as total_clicks,
      sum(metrics__impressions) as total_impressions
  from googleadsapi.campaign_performance_reports cost_data
  inner join(
      select *
      from
        (select distinct 
          campaign__id as id, 
          campaign__name as name, 
          split_part(name, '_|_', 8) as product,
          row_number() over (partition by campaign__id order by segments__date desc) as rn
        from googleadsapi.campaigns)
      where rn = 1
    ) campaign_names on cost_data.campaign__id = campaign_names.id
  where true
    and segments__date >= '2020-01-01' 
  group by 1,2,3) new_data

on old_data.date_stop = new_data.date_stop and old_data.campaign_id = new_data.campaign_id --and old_data.name = new_data.name