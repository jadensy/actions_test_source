/*
MAIN: main script for Appsflyer Install
*/


WITH date_select AS (
    SELECT
        [daterange_start] start_date,
        [daterange_end] end_date
),
ffa_membership_with_aff as (

    select
        userid,
        email,
        first_date as ffa_with_aff_date,
        first_timestamp as ffa_with_aff_timestamp,
        product as ffa_with_aff_product,
        product_detail as ffa_with_aff_product_detail,
        source as ffa_with_aff_source,
        source_breakdown as ffa_with_aff_source_breakdown,
        rn as rn_ffa_with_aff,
        last_update_timestamp
    from
        marketing.stg_user__membership__merged_with_aff
    where
        product not in ('lite', 'new core')

),

------ install attribution ------
install_attribution AS (
    SELECT
        appsflyer_id,
        platform,
        event_time,
        touch_sequence,
        source,
        ad_platform,
        app_priority_rank,
        install_date,
        install_timestamp,
        ml_user_id,
        user_email,
        lite_date,
        lite_timestamp,
        lite_source,
        core_date,
        core_timestamp,
        core_source,
        ffa_date,
        ffa_timestamp,
        rn_ffa,
        ffa_product,
        ffa_source,
        lite_register
    FROM
        marketing.fct_install_attribution_model__install_last_touch a
        CROSS JOIN date_select
    WHERE
        install_date >= date_select.start_date
        AND install_date <= date_select.end_date
--         AND (
--             rn_ffa IS NULL
--             OR rn_ffa = 1
--         )
        AND touch_sequence IN ('last touch', 'organic') 
        ------ exclude pre-signup ------
        AND lite_register IN ('no_lite', 'mobileApp') 
        ------ exclude install campaign ------
        AND ad_platform not in ('blindferret', 'tapjoy')
),

install_with_aff AS (
  SELECT
        appsflyer_id,
        platform,
        event_time,
        touch_sequence,
        source,
        ad_platform,
        app_priority_rank,
        install_date,
        install_timestamp,
        ml_user_id,
        user_email,
        lite_date,
        lite_timestamp,
        lite_source,
        core_date,
        core_timestamp,
        core_source,
        ffa_date,
        ffa_timestamp,
        rn_ffa,
        ffa_product,
        ffa_source,
        ffa_with_aff_date,
        ffa_with_aff_timestamp,
        rn_ffa_with_aff,
        ffa_with_aff_product,
        ffa_with_aff_source,
        lite_register
 FROM
        install_attribution a
        LEFT JOIN ffa_membership_with_aff b on a.ml_user_id=b.userid
--   WHERE
--        (
--             rn_ffa_with_aff IS NULL
--             OR rn_ffa_with_aff = 1
--         )
  ),


------ appsflyer marketing bucket -----
marketing_bucket as (
    select
        marketing_bucket,
        marketing_source
    from 
        [appsflyer_marketing_bucket]
),

------ install model -----
install_model as (
    select
        install_with_aff.*,
        marketing_bucket.marketing_bucket

    from 
        install_with_aff
        left join marketing_bucket on install_with_aff.source = marketing_bucket.marketing_source
),

------ calculation ------
final AS (
    SELECT
        ffa_with_aff_product as ffa_product,
        count(appsflyer_id) install,
        -- count(lite_date) lite,
        -- count(core_date) core,
        count(ffa_with_aff_date) ffa
    FROM
        install_model
    where
        [marketing_bucket=marketing_bucket]
    GROUP BY
        1
),
total as (
    select
        sum(ffa) total_ffa
    from final 
    where ffa_product is not null
)
SELECT
    ffa_product,
    ffa,
    (1.00 * ffa / total_ffa) ffa_pct
FROM
    final
    cross join total
where
    ffa_product is not null
order by 2 desc