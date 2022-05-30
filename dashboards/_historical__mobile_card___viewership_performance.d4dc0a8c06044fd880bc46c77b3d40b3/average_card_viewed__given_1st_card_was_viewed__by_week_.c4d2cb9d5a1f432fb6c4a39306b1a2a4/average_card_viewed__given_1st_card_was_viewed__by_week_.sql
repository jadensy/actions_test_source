select [timestamp:week] as week, 
       avg((second_viewed + third_viewed + fourth_viewed)::float/3)
from [card_performance]
where first_viewed = 1
group by 1
order by 1 desc