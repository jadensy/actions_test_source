--no_cache
with canvas as (
  select distinct canvas_id
  from brazeapi.canvas_tag
  where "tag" = 'journey'
  )

select
  a.canvas_name
  , sent
  , delivered
  , opened
  , clicked
  , opened * 1.0 / delivered as open_rate
  , clicked * 1.0 / opened as click_rate

  , direct_debitcardactivationresult dir_cardactivation
  , direct_debitcardactivationresult * 1.0 / clicked as dir_cardactivation_rate

  , indirect_debitcardactivationresult indir_cardactivation
  , direct_debitcardactivationresult * 1.0 / opened as indir_cardactivation_rate

  , dir_cardactivation + indir_cardactivation as total_cardactivation

  , bounced
  , spam
  , unsub
  , unsub * 1.0 / opened as unsub_rate

FROM (select canvas_name, count(distinct lower(user_id)) as sent from braze.email_sent where canvas_id in (select * from canvas) group by 1) AS a
LEFT JOIN (select canvas_name, count(distinct lower(user_id)) as delivered from braze.email_delivered where canvas_id in (select * from canvas) group by 1) AS b ON a.canvas_name = b.canvas_name
LEFT JOIN (select canvas_name, count(distinct lower(user_id)) as opened from braze.email_opened where canvas_id in (select * from canvas) group by 1) AS c ON a.canvas_name = c.canvas_name
LEFT JOIN (select canvas_name, count(distinct lower(user_id)) as clicked from braze.email_link_clicked where canvas_id in (select * from canvas) group by 1) AS d ON a.canvas_name = d.canvas_name
LEFT JOIN (select canvas_name, count(distinct lower(user_id)) as bounced from braze.email_bounced where canvas_id in (select * from canvas) group by 1) AS e ON a.canvas_name = e.canvas_name
LEFT JOIN (select canvas_name, count(distinct lower(user_id)) as spam from braze.email_marked_as_spam where canvas_id in (select * from canvas) group by 1) AS f ON a.canvas_name = f.canvas_name
LEFT JOIN (select canvas_name, count(distinct lower(user_id)) as unsub from braze.unsubscribed where canvas_id in (select * from canvas) group by 1) AS g ON a.canvas_name = g.canvas_name
LEFT JOIN ( -- Direct Conversion Card Activation
  select distinct
    belc.canvas_id
  , belc.canvas_name
  , count(distinct case when dcar.timestamp is not null then dcar.user_id end) direct_debitcardactivationresult
  from braze.email_link_clicked belc
  left join prod.debit_card_activation_result dcar on belc.user_id = dcar.user_id and belc.timestamp <= dcar.timestamp and datediff(hour, belc.timestamp, dcar.timestamp) <= 24
  where result = 'success'
  and belc.canvas_id in (select * from canvas)
  group by 1,2
  ) AS h ON a.canvas_name = h.canvas_name
LEFT JOIN ( -- Indirect Conversion Card Activation
  select distinct
    beo.canvas_id
  , beo.canvas_name
  , count(distinct case when dcar.timestamp is not null then dcar.user_id end) indirect_debitcardactivationresult
  from braze.email_opened beo
  left join prod.debit_card_activation_result dcar on beo.user_id = dcar.user_id and beo.timestamp <= dcar.timestamp and datediff(hour, beo.timestamp, dcar.timestamp) <= 24
  where result = 'success'
  and beo.canvas_id in (select * from canvas)
  group by 1,2
  ) AS i ON a.canvas_name = i.canvas_name
ORDER BY 1