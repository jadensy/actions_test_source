WITH date_select AS (
    SELECT
        [daterange_start] start_date,
        [daterange_end] end_date
),
------ install attribution ------
install_model AS (
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
        marketing.fct_install_attribution_model__install_last_touch
        CROSS JOIN date_select
    WHERE
        install_date >= date_select.start_date
        AND install_date <= date_select.end_date
        AND (
            rn_ffa IS NULL
            OR rn_ffa = 1
        )
        AND touch_sequence IN ('last touch', 'organic')
),
------ calculation ------
final AS (
    SELECT
        [install_date:aggregation] dt,
        ad_platform,
        count(appsflyer_id) install
    FROM
        install_model
    WHERE
        source = 'performance marketing'
        AND lite_register IN ('no_lite', 'mobileApp')
    GROUP BY
        1,
        2
)
SELECT
    *
FROM
    final
ORDER BY
    1 DESC