--no_cache
with canvas as (
  select distinct canvas_id
  from brazeapi.canvas_tag
  where "tag" = 'journey'
  )

select
  a.canvas_name
  , sent
  , tapped
  , bounced

  , direct_debitcardactivationresult dir_cardactivation
  , direct_debitcardactivationresult * 1.0 / tapped as dir_cardactivation_rate

  , indirect_debitcardactivationresult indir_cardactivation
  , direct_debitcardactivationresult * 1.0 / sent as indir_cardactivation_rate

  , dir_cardactivation + indir_cardactivation as total_cardactivation

FROM (select canvas_name, count(distinct lower(user_id)) as sent from braze.push_notification_sent where canvas_id in (select * from canvas) group by 1) AS a
LEFT JOIN (select canvas_name, count(distinct lower(user_id)) as tapped from braze.push_notification_tapped where canvas_id in (select * from canvas) group by 1) AS b ON a.canvas_name = b.canvas_name
LEFT JOIN (select canvas_name, count(distinct lower(user_id)) as bounced from braze.push_notification_bounced where canvas_id in (select * from canvas) group by 1) AS c ON a.canvas_name = c.canvas_name
LEFT JOIN ( -- Direct Conversion Card Activation
  select distinct
    bpnt.canvas_id
  , bpnt.canvas_name
  , count(distinct case when dcar.timestamp is not null then dcar.user_id end) direct_debitcardactivationresult
  from braze.push_notification_tapped bpnt
  left join prod.debit_card_activation_result dcar on bpnt.user_id = dcar.user_id and bpnt.timestamp <= dcar.timestamp and datediff(hour, bpnt.timestamp, dcar.timestamp) <= 24
  where result = 'success'
  and bpnt.canvas_id in (select * from canvas)
  group by 1,2
  ) AS h ON a.canvas_name = h.canvas_name
LEFT JOIN ( -- Indirect Conversion Card Activation
  select distinct
    bpns.canvas_id
  , bpns.canvas_name
  , count(distinct case when dcar.timestamp is not null then dcar.user_id end) indirect_debitcardactivationresult
  from braze.push_notification_sent bpns
  left join prod.debit_card_activation_result dcar on bpns.user_id = dcar.user_id and bpns.timestamp <= dcar.timestamp and datediff(hour, bpns.timestamp, dcar.timestamp) <= 24
  where result = 'success'
  and bpns.canvas_id in (select * from canvas)
  group by 1,2
  ) AS i ON a.canvas_name = i.canvas_name
ORDER BY 1