select distinct 
  ctag.canvas_id
, cent.*
from brazeapi.canvas_tag ctag
left join braze.canvas_entered cent on ctag.canvas_id = cent.canvas_id
where "tag" = 'journey'