--no_cache
with RM_call as (
select a.call_id, date(created_at) as contact_date, rating * 1.0 as rating
from ujetapi.call as a 
left join ujetapi.call_menu_path as b on a.call_id = b.call_id
where call_type = 'IvrCall'
and status != 'deflected'
and date(created_at) >= '2020-06-01' 
and rating > 0 
and rating is not null
and b.name like '%Metabank%'
),

RM_csat as (
select [contact_date:week] as reporting_week, 
avg(rating) as "voice_service_csat"
from RM_call
group by 1)


select reporting_week, voice_service_csat, (voice_service_csat * 1.0 / 5) as voice_csat_standardized
from RM_csat a 
where reporting_week > current_date - 60
and reporting_week < current_date - 5
order by 1 desc