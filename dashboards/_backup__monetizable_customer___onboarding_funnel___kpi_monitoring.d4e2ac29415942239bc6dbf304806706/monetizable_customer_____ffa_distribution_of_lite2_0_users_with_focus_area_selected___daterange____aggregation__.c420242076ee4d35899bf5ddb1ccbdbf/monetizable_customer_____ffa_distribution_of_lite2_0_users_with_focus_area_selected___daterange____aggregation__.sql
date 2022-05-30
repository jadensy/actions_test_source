/*
MAIN: FFA ANALYSIS
*/

WITH date_select AS (
    SELECT
        '2022-01-25' :: date start_date,
        ([daterange_end] - INTERVAL '3 day') :: date end_date
),

gp_selection_focus_area as (
    select
        userid,
        email,
        lite_timestamp,
        screen_timestamp
    from
        [gp_selection_focus_area]
        cross join date_select
    where
        gp_user_id is not null
        and lite_timestamp :: date >= date_select.start_date
        and lite_timestamp :: date <= date_select.end_date
),

------ cohort ffa from backend membership ------
ffa_membership as (
    SELECT
        userid,
        email,
        first_date,
        first_timestamp,
        product,
        product_detail,
        source,
        source_breakdown,
        last_update_timestamp,
        rn
    FROM
        marketing.stg_user__membership__merged_with_aff
        CROSS JOIN date_select
    WHERE
        product not in ('lite', 'new core')
        and rn = 1
        and first_date >= date_select.start_date
),

cohort_ffa as (
    select
        gp_selection_focus_area.userid, 
        gp_selection_focus_area.email,
        gp_selection_focus_area.lite_timestamp,
        gp_selection_focus_area.screen_timestamp,

        ffa_membership.product,
        ffa_membership.first_timestamp as ffa_timestamp,
        ffa_membership.rn as rn_ffa
        
    from 
        gp_selection_focus_area
        left join ffa_membership on gp_selection_focus_area.userid = ffa_membership.userid
),

------ calculation ------
ffa_summary as (
    select
        [lite_timestamp:aggregation] as dt,
        product, 
        count(email) ffa_count
    from 
        cohort_ffa
    where
        product is not null
    group by 1, 2
),

daily_total as (
    select
        dt,
        sum(ffa_count) total_ffa
    from 
        ffa_summary
    group by 1
),

final as (
    select
        ffa_summary.dt,
        product,
        ffa_count,
        (1.00 * ffa_count / total_ffa) ffa_pct
    from 
        ffa_summary
        left join daily_total on ffa_summary.dt = daily_total.dt
)

select * from final
order by 1