WITH date_select AS (
    SELECT
        [daterange_start] start_date,
        [daterange_end] end_date
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
        [install_with_aff]
  ),

------ calculation ------
final AS (
    SELECT
        [install_date:aggregation] dt,
        count(appsflyer_id) install,
        count(lite_date) lite,
        count(core_date) core,
        count(ffa_with_aff_date) ffa
    FROM
        install_with_aff
    GROUP BY
        1
)
SELECT
    dt,
    install,
    (1.00 * lite / install) install_to_lite,
    (1.00 * core / install) install_to_core,
    (1.00 * ffa / install) install_to_ffa
FROM
    final
ORDER BY
    1 DESC