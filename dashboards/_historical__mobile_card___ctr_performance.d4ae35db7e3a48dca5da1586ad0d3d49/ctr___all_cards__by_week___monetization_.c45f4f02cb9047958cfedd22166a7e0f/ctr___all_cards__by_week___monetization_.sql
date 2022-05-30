/* 
select [timestamp:week] as week,
       Case when monetized = 1 then 'Monetized' else 'Non-Monetized' end as monetized,
        sum(is_viewed) as viewed_cards,
        sum(is_engaged) as clicked_cards,
        isnull( Cast(sum(is_engaged) as float) / 
               NULLIF(sum(is_viewed), 0), 0) as CTR
from
(
  select card, monetized, is_viewed, is_engaged, timestamp
  from
  (
    select first as card, first_viewed as is_viewed, first_engaged as is_engaged, timestamp
    from [card_performance]

    union all

    select second as card, second_viewed as is_viewed, second_engaged as is_engaged, timestamp
    from [card_performance]

    union all

    select third as card, third_viewed as is_viewed, third_engaged as is_engaged, timestamp
    from [card_performance]

    union all

    select fourth as card, fourth_viewed as is_viewed, fourth_engaged as is_engaged, timestamp
    from [card_performance]
  ) as card_data
  left join
  usr_rsulca.cards_master_list
  on card_data.card = cards_master_list.key
)
group by 1, 2
order by 1 desc, 2 */