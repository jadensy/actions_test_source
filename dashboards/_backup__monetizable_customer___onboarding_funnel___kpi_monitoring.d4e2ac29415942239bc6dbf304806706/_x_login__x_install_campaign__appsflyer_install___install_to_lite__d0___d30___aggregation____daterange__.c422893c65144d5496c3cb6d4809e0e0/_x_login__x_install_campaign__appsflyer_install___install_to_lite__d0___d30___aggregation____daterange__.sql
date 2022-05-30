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


------ calculation ------
final AS (
    SELECT
        [install_date:aggregation] dt,
        count(appsflyer_id) install,
        count(distinct user_email) lite,
        count(distinct case when (lite_date-install_date)='0' then appsflyer_id end) as D0,
        count(distinct case when (lite_date-install_date)>='0' and (lite_date-install_date)<='30' then appsflyer_id end) as D30
    FROM
        install_attribution
    GROUP BY
        1
)
SELECT
    dt,
    install,
    lite,
    (1.00 * D0 / install) "%D0",
    (1.00 * D30 / install) "%D30"
FROM
    final
ORDER BY
    1 DESC