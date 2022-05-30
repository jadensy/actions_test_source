with base as (
SELECT
  id,
  [created_at:week] as report_week,
  INITCAP(replace(replace(replace(disposition_2_0_bnpl, '_', ' '), '34', ''),'2', '')) as disposition
from zendesk.tickets
where disposition_2_0_product = 'bnpl_-_buy_now_pay_later'
and created_at > current_date - 45
),

base2 as (
SELECT 
  disposition,
  count(case when report_week < current_date - 6 and report_week > current_date - 12 then id end) as vol_lastweek,
  count(case when report_week < current_date - 13 and report_week > current_date - 19 then id end) as vol_prevweek
from base
group by 1
)

SELECT 
  disposition,
  vol_lastweek as "Volume Last Week",
  vol_prevweek as "Previous Volume",
  ((vol_lastweek - vol_prevweek) * 1.0) / nullif(vol_prevweek,0) as "% Change"
from base2
where disposition is not null
order by 2 desc