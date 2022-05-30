WITH login_relink AS (
  SELECT client_id
  , json_extract_path_text(resp, 'bankId') AS bank_id
  , created_on
  , event_name
  , api_code
  , source
  , id
  FROM ml_public.mysql_bv_log
  WHERE created_on >= '2021-01-01'
  AND event_name IN ('login', 'relink') 
  AND status = 200
  )

, unlink AS (
  SELECT client_id
  , json_extract_path_text(req, 'bankId') AS bank_id
  , created_on
  , event_name
  , api_code
  , source
  , id
  , lead (created_on) OVER (PARTITION BY bank_id ORDER BY id) AS next_event_time
  FROM ml_public.mysql_bv_log
  WHERE created_on >= '2021-01-01'
  AND event_name IN ('unlink') 
  AND status = 200
  )

, error AS (
  SELECT client_id
  , lead(SPLIT_PART(SPLIT_PART(req, '"bankId":"', 2), '"', 1)) OVER (ORDER BY id) AS next_bank_id
--   , lead (json_extract_path_text(req, 'bankId')) OVER (ORDER BY id) AS next_bank_id
  , created_on
  , event_name
  , api_code
  , source
  , id
  , lead (event_name) OVER (ORDER BY id) AS next_event_name
  FROM ml_public.mysql_bv_log
  WHERE created_on >= '2021-01-01'
  AND ((event_name IN ('unlink') AND status = 200) OR api_code IN ('ITEM_LOGIN_REQUIRED'))
  )

, base AS (
  SELECT client_id
  , bank_id
  , created_on
  , event_name
  , api_code
  , source
  , id
  FROM login_relink
  UNION
  SELECT client_id
  , bank_id
  , created_on
  , event_name
  , api_code
  , source
  , id
  FROM unlink
  WHERE nvl(datediff('second', created_on, next_event_time),999) >= 2
  UNION
  SELECT client_id
  , next_bank_id AS bank_id
  , created_on
  , event_name
  , api_code
  , source
  , id
  FROM error
  WHERE event_name <> 'unlink' 
  AND next_event_name = 'unlink'
  )

, main AS (
  SELECT client_id
  , bank_id
  , created_on AS event_time
  , event_name
  , api_code AS event_code
  , source AS event_source
  , lag (created_on) OVER (PARTITION BY bank_id ORDER BY id) AS last_event_time
  , nvl(lag (event_name) OVER (PARTITION BY bank_id ORDER BY id), '') AS last_event_name
  , nvl(lag (api_code) OVER (PARTITION BY bank_id ORDER BY id), '') AS last_event_code
  , nvl(lag (source) OVER (PARTITION BY bank_id ORDER BY id), '') AS last_event_source
  , nvl(lead (created_on) OVER (PARTITION BY bank_id ORDER BY id), current_date+2) AS next_event_time
  , nvl(lead (event_name) OVER (PARTITION BY bank_id ORDER BY id), '') AS next_event_name
  , nvl(lead (api_code) OVER (PARTITION BY bank_id ORDER BY id), '') AS next_event_code
  , nvl(lead (source) OVER (PARTITION BY bank_id ORDER BY id), '') AS next_event_source
  , datediff('second', last_event_time, created_on) AS ts_diff_sec
  , datediff('minute', last_event_time, created_on) AS last_event_min_diff
  , datediff('minute', created_on, next_event_time) AS next_event_min_diff
  , CASE
      WHEN event_name = last_event_name AND ts_diff_sec < 2 THEN true
      ELSE false
    END AS is_duplicate
  FROM base
  )

SELECT client_id
, bank_id
, event_time
, event_name 
, event_code
, event_source
, last_event_time
, last_event_name
, last_event_code
, last_event_source
, last_event_min_diff
, next_event_time
, next_event_name
, next_event_code
, next_event_source
, next_event_min_diff
FROM main
WHERE is_duplicate = false