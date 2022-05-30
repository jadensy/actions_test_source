with base as (

  select

   db.id as db_id
  , db.name as db_name
  , db.created_at as db_created_at
  , db.created_by as db_created_by
  , db.deleted_at as db_deleted_at
  , db.owner_id as db_owner_id
  , db.space_id as db_space_id
  , db.last_used_at as db_last_used_at
  , DATEDIFF( 'day', db.last_used_at, NOW() ) as db_days_since_last_used
  , db.archived_at as db_archived_at
  , db.will_archive_at as db_will_archive_at
  , case when db.deleted_at is not null then TRUE else FALSE end as db_is_deleted
  , case when db.archived_at is not null then TRUE else FALSE end as db_is_archived
  , case when db.archived_at is not null or db.deleted_at is not null or DATEDIFF( 'day', NOW(), db.will_archive_at ) < 0 then 'INACTIVE' else 'ACTIVE' end as db_status
  , DATEDIFF( 'day', db.deleted_at, NOW() ) as db_days_since_deleted
  , DATEDIFF( 'day', db.archived_at, NOW() ) as db_days_since_archive
  , DATEDIFF( 'day', NOW(), db.will_archive_at ) as db_days_till_archive
  , usr.id as db_owner_id__usr_id
  , usr.first_name as db_owner_id__usr_first_name
  , usr.last_name as db_owner_id__usr_last_name
  , usr.email_address as db_owner_id__usr_email_address
  , usr2.id as db_created_by__usr_id
  , usr2.first_name as db_created_by__usr_first_name
  , usr2.last_name as db_created_by__usr_last_name
  , usr2.email_address as db_created_by__usr_email_address
  , sp.id as sp_id
  , sp.name as sp_name
  , sp.created_at as sp_created_at
  , sp.created_by as sp_created_by
  , sp.deleted_at as sp_deleted_at
  , case when sp.deleted_at is not null then TRUE else FALSE end as sp_is_deleted



from
   periscope_usage_data.dashboards as db
  left join periscope_usage_data.users as usr on usr.id = db.owner_id
  left join periscope_usage_data.users as usr2 on usr2.id = db.created_by
  left join periscope_usage_data.spaces as sp on sp.id = db.space_id

-- For debugging
-- where db.id = 'ba1be3b2d247b41a'
  
)

select 

 sp_name
, db_name
, db_id
, db_owner_id__usr_first_name || ' ' || db_owner_id__usr_last_name as db_owner
, db_created_by__usr_first_name || ' ' ||  db_created_by__usr_last_name as db_creator
-- , db_created_by__usr_email as db_creater
-- , db_owner_id__usr_email_address as db_owner
, DATE(db_created_at) as db_created_at__date
, DATE(db_last_used_at) as db_last_used_at__date
, DATE(db_will_archive_at) as db_will_archive_at__date
, db_days_since_last_used
, db_days_till_archive

from base
where 
db_status = 'ACTIVE'
and sp_is_deleted is false

order by sp_name asc