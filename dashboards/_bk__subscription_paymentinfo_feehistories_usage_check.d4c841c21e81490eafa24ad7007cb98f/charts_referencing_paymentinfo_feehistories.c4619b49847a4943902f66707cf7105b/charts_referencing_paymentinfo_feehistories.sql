with all_queries as (
  select
    c.id
    , 'chart' as type
    , c.name
    , c.space_id
    , c.sql
    , c.created_at
    , c.created_by
    , c.updated_at
  from periscope_usage_data.charts as c
  left join periscope_usage_data.dashboards as d
    on d.id = c.dashboard_id
  where c.deleted_at is null
    and d.deleted_at is null
    and d.archived_at is null

  UNION ALL

  select
    id
    , 'view' as type
    , name
    , null
    , sql
    , created_at
    , created_by
    , updated_at
  from periscope_usage_data.views
  where deleted_at is null
    and archived_at is null

  UNION ALL

  select
    id
    , 'snippet' as type
    , name
    , space_id
    , sql
    , created_at
    , created_by
    , updated_at
  from periscope_usage_data.sql_snippets
  where deleted_at is null
)

select
  aq.id
  , aq.type
  , aq.name
  , s.name as space
  , aq.sql
  , (u.first_name || ' ' || u.last_name) as created_by
  , aq.created_at
  , aq.updated_at
  , regexp_substr(aq.sql, '\\w{4,}\\.\\w{3,}\\.\\w{3,}') as potential_crossdb
from all_queries as aq
left join periscope_usage_data.users as u
  on u.id = aq.created_by
left join periscope_usage_data.spaces as s
  on s.id = aq.space_id
-- where aq.sql ~* 'feeHistories'
where aq.sql ~* 'paymentinfo_feehistories'
order by created_at