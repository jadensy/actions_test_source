(
  select distinct key
  from usr_rsulca.cards_master_list
  where lower(sub_category) in ('ad', 'advertisement')
)