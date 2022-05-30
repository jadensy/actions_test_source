--no_cache
WITH tags AS (SELECT *
              FROM brazeapi.canvas a
              LEFT JOIN brazeapi.canvas_tag b ON a.id = b.canvas_id
              WHERE "tag" = 'journey')

SELECT a.canvas_id, b."tag", b.name, [sent_at:date] as sent_at, count(*) as sent
FROM braze.push_notification_sent a
LEFT JOIN tags b ON a.canvas_id = b.canvas_id
WHERE a.canvas_id IN (SELECT id FROM tags)
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4