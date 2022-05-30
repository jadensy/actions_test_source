--no_cache
with canvas as (
  select distinct canvas_id
  from brazeapi.canvas_tag
  where "tag" = 'journey'
  )

select
  a.canvas_name
  , viewed impression
  , clicked_button0 button1_clicks
  , clicked_button1 button2_clicks

  , direct_debitcardactivationresult dir_cardactivation
  , direct_debitcardactivationresult * 1.0 / button2_clicks as dir_cardactivation_rate

  , indirect_debitcardactivationresult indir_cardactivation
  , direct_debitcardactivationresult * 1.0 / impression as indir_cardactivation_rate

  , dir_cardactivation + indir_cardactivation as total_cardactivation

FROM (select canvas_name, count(distinct lower(user_id)) as viewed from braze.in_app_message_viewed where canvas_id in (select * from canvas) group by 1) AS a
LEFT JOIN (select canvas_name, count(distinct case when button_id = 0 then lower(user_id) end) as clicked_button0 from braze.in_app_message_clicked where canvas_id in (select * from canvas) group by 1) AS b ON a.canvas_name = b.canvas_name
LEFT JOIN (select canvas_name, count(distinct case when button_id = 1 then lower(user_id) end) as clicked_button1 from braze.in_app_message_clicked where canvas_id in (select * from canvas) group by 1) AS c ON a.canvas_name = c.canvas_name
LEFT JOIN ( -- Direct Conversion Card Activation
  select distinct
    biamc.canvas_id
  , biamc.canvas_name
  , count(distinct case when dcar.timestamp is not null then dcar.user_id end) direct_debitcardactivationresult
  from braze.in_app_message_clicked biamc
  left join prod.debit_card_activation_result dcar on biamc.user_id = dcar.user_id and biamc.timestamp <= dcar.timestamp and datediff(hour, biamc.timestamp, dcar.timestamp) <= 24
  where result = 'success'
  and biamc.canvas_id in (select * from canvas)
  group by 1,2
  ) AS h ON a.canvas_name = h.canvas_name
LEFT JOIN ( -- Indirect Conversion Card Activation
  select distinct
    biamv.canvas_id
  , biamv.canvas_name
  , count(distinct case when dcar.timestamp is not null then dcar.user_id end) indirect_debitcardactivationresult
  from braze.in_app_message_viewed biamv
  left join prod.debit_card_activation_result dcar on biamv.user_id = dcar.user_id and biamv.timestamp <= dcar.timestamp and datediff(hour, biamv.timestamp, dcar.timestamp) <= 24
  where result = 'success'
  and biamv.canvas_id in (select * from canvas)
  group by 1,2
  ) AS i ON a.canvas_name = i.canvas_name
ORDER BY 1