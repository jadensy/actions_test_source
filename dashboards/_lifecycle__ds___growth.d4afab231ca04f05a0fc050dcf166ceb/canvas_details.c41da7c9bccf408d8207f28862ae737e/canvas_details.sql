select distinct 
  ctag.canvas_id
, cdet.name
, sum(cdata.entries) total_entries
, sum(cdata.revenue) total_revenue
from brazeapi.canvas_tag ctag
left join brazeapi.canvas cdet on ctag.canvas_id = cdet.id
left join brazeapi.canvas_data_series cdata on cdet.id = cdata.id
where "tag" = 'journey'
group by 1,2
order by 2