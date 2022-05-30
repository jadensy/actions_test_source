/*
main: focus area analysis
*/

WITH date_select AS (
    SELECT
        '2022-01-25' :: date start_date,
        ([daterange_end] - INTERVAL '3 day') :: date end_date
),

gp_selection_focus_area_key as (
    select
        userid,
        email,
        lite_timestamp,
        screen_timestamp,
        gp_user_id,
        index,
        focus_area_key,
        dwh_timestamp
    from
        [gp_selection_focus_area_key]
        cross join date_select
    where
        focus_area_key is not null
        and lite_timestamp :: date >= date_select.start_date
        and lite_timestamp :: date <= date_select.end_date
),

------ calculation ------
select_summary as (
    select
        [screen_timestamp:aggregation] as dt,
        focus_area_key,
        count(email) select_count
    from 
        gp_selection_focus_area_key
    group by 1, 2
),

daily_total as (
    select
        dt,
        sum(select_count) total_select
    from 
        select_summary
    group by 1
),

final as (
    select
        select_summary.dt,
        focus_area_key,
        select_count,
        (1.00 * select_count / total_select) select_pct
    from 
        select_summary
        left join daily_total on select_summary.dt = daily_total.dt
)

select * 
from final  
order by 1