with adb as (
  
  select * from [all_active_dashboards_with_owners]

)

, tc as (
  
  select * from [total_charts_per_dashboard]

)

select 

  adb.sp_name as space
, adb.db_name as name
, adb.db_id as id
, adb.db_creator as creator
, adb.db_owner as owner
, adb.db_created_at__date as create_at
, adb.db_last_used_at__date as last_used_at
, adb.db_will_archive_at__date as will_archive_at
, adb.db_days_since_last_used as days_since_last_used
, adb.db_days_till_archive as days_till_archive
, tc.ct_earliest_created_date as earliest_chart_created
, tc.ct_latest_created_date as latest_chart_created
, tc.ct_earliest_updated_date as earliest_chart_updated
, tc.ct_latest_updated_date as latest_chart_updated
, tc.total_charts as total_charts

from adb
left join tc on tc.db_id = adb.db_id

where adb.db_id not in (
  '1fae7e9bd33fee97' -- BK's Dashboard
)

order by 
  space asc
  , days_since_last_used asc
  , latest_chart_updated desc
  , latest_chart_created desc