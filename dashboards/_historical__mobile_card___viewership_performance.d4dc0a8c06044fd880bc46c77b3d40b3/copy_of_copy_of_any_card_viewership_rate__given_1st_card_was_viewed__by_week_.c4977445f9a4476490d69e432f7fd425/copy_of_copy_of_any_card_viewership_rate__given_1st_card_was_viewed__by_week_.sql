select [timestamp:week] as week, 
       sum(case when fourth_viewed = 1 then 1 else 0 end) / Cast(sum(first_viewed) as float) as VTR
from [card_performance]
where first_viewed = 1
group by 1
order by 1 desc