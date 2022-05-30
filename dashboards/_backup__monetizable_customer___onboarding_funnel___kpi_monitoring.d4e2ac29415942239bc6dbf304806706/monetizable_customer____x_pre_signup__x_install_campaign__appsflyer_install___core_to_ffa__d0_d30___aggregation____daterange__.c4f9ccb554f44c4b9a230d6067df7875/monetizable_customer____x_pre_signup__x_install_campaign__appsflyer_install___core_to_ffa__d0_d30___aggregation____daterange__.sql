/*
MAIN: main script for Appsflyer Install
*/


WITH date_select AS (
    SELECT
        [daterange_start] start_date,
        [daterange_end] end_date
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
        ffa_with_aff_date,
        ffa_with_aff_timestamp,
        rn_ffa_with_aff,
        ffa_with_aff_product,
        ffa_with_aff_source,
        lite_register
 FROM
        [install_with_aff]
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
        install_attribution.*,
        marketing_bucket.marketing_bucket

    from 
        install_attribution
        left join marketing_bucket on install_attribution.source = marketing_bucket.marketing_source
),

------ calculation ------
final AS (
    SELECT
        [install_date:aggregation] dt,
        count(appsflyer_id) install,
        count(lite_date) lite,
        count(core_date) core,
        count(ffa_with_aff_date) ffa,
        count(distinct case when (ffa_with_aff_date-core_date)='0' then user_email end) as D0,
        count(distinct case when (ffa_with_aff_date-core_date)>='0' and (ffa_with_aff_date-core_date)<='30' then user_email end) as D30
    FROM
        install_model
    GROUP BY
        1
) 
SELECT
    dt,
    core,
    (1.00 * D0 / core) "%D0",
    (1.00 * D30 / core) "%D30"
FROM
    final