WITH tags AS (SELECT a.*, 
                     b.campaign_tag
              FROM brazeapi.campaign_details a
              LEFT JOIN brazeapi.campaign_details_tags b ON a.campaign_id = b.campaign_id
              WHERE campaign_tag ~ '^instacash/' 
                AND draft = False
                AND name IS NOT NULL
                AND last_sent >= current_date - interval '2 month')

   , sent AS (SELECT campaign_id, count(*) as sent
              FROM braze.push_notification_sent
              WHERE campaign_id IN (SELECT campaign_id FROM tags)
              GROUP BY campaign_id )

 , tapped AS (SELECT campaign_id, count(*) as tapped
              FROM braze.push_notification_tapped
              WHERE campaign_id IN (SELECT campaign_id FROM tags) 
              GROUP BY campaign_id )

, bounced AS (SELECT campaign_id, count(*) as bounced
              FROM braze.push_notification_bounced
              WHERE campaign_id IN (SELECT campaign_id FROM tags) 
              GROUP BY campaign_id )

, join_tab AS (SELECT a.campaign_tag,
                      a.campaign_id,
                      a.name as campaign_name,
                      sent, 
                      tapped, 
                      bounced
               FROM tags a
               LEFT JOIN sent b ON a.campaign_id = b.campaign_id
               LEFT JOIN tapped c ON a.campaign_id = c.campaign_id
               LEFT JOIN bounced d ON a.campaign_id = d.campaign_id )

, ca_appl AS (SELECT * 
              FROM (SELECT user_id, 
                           timestamp as cash_adv_request,
                           result,
                           cash_advance_amount,
                           ROW_NUMBER() OVER (PARTITION BY user_id, [timestamp:date] ORDER BY "timestamp") as rank
                    FROM prod.ddacash_advance_request)
              WHERE rank = 1)

, dat_sent AS (SELECT campaign_id, campaign_name, user_id, "timestamp" as sent
               FROM (SELECT *, 
                            ROW_NUMBER() OVER (PARTITION BY campaign_name, user_id, [timestamp:date] ORDER BY "timestamp" DESC) as rank
                     FROM braze.push_notification_sent)
               WHERE rank = 1)

, dat_tapped AS (SELECT campaign_id, campaign_name, user_id, "timestamp" as tapped
                 FROM (SELECT *, 
                              ROW_NUMBER() OVER (PARTITION BY campaign_name, user_id, [timestamp:date] ORDER BY "timestamp" DESC) as rank
                       FROM braze.push_notification_tapped)
                 WHERE rank = 1)

, conversion AS (SELECT a.campaign_id, a.campaign_name,
                        count(*) as sent_ct,
                        sum(CASE WHEN tapped IS NOT NULL THEN 1 ELSE 0 END) as tapped_ct,
                        sum(CASE WHEN cash_adv_request IS NOT NULL AND tapped IS NOT NULL THEN 1 ELSE 0 END) as direct_conversion_ct,
                        sum(CASE WHEN cash_adv_request IS NOT NULL AND tapped IS NULL THEN 1 ELSE 0 END) as indirect_conversion_ct
                 FROM dat_sent a
                 LEFT JOIN dat_tapped b 
                        ON a.user_id = b.user_id 
                           AND b.tapped BETWEEN a.sent AND (a.sent + interval '1 day')        
                           AND a.campaign_name = b.campaign_name 
                 LEFT JOIN ca_appl c 
                        ON a.user_id = c.user_id 
                           AND c.cash_adv_request BETWEEN a.sent AND (a.sent + interval '1 day')
                 GROUP BY 1,2)

, conv_rate AS (SELECT campaign_id,
                       max(campaign_name),
                       sum(sent_ct) as sent,
                       sum(tapped_ct) as tapped,
                       sum(direct_conversion_ct) as direct_conversion,
                       sum(indirect_conversion_ct) as indirect_conversion,
                       CAST(direct_conversion AS float)/ sent as dir_conv_rate,
                       CAST(indirect_conversion AS float)/ sent as indir_conv_rate
                FROM conversion
                GROUP BY 1)

SELECT a.campaign_tag,
       a.campaign_name,
       a.sent,
       a.tapped,
       a.bounced,
       b.direct_conversion as dir_conv, 
       b.dir_conv_rate, 
       b.indirect_conversion as indir_conv, 
       b.indir_conv_rate,
       dir_conv + indir_conv as total_conv,
       b.dir_conv_rate + b.indir_conv_rate as total_conv_rate
FROM join_tab a
LEFT JOIN conv_rate b
       ON a.campaign_id = b.campaign_id
ORDER BY campaign_tag