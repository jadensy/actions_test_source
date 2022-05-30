/*
MAIN: main script for Appsflyer Install
*/


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

------ appsflyer marketing bucket -----
marketing_bucket as (
    select
        marketing_bucket,
        marketing_source
    from 
        [appsflyer_marketing_bucket]
),

------ onboarding checkpoint ------
core_onboarding as (
    select
        anonymous_id,
        platform,
        timezone,
        event,
        flow,
        name as event_name,
        inactivity_min,
        user_id,
        checkpoint_date,
        checkpoint_timestamp,
        core_timestamp,
        core_source,
        is_before_core_created
    from 
        marketing.stg_onboarding_funnel__core_onboarding_event onboarding_event
),

core_journey as (
    select 
    *
    from (
      select
          user_id,
          min(case when event_name = 'Core Onboarding Intro' then checkpoint_timestamp end) core_intro
      from 
          core_onboarding
      group by 1
    )
    cross join date_select
    where 
        core_intro>=date_select.start_date
        AND core_intro <= date_select.end_date
    
),

------ install model -----
install_model as (
    select
        install_with_aff.*,
        marketing_bucket.marketing_bucket,
        core_journey.core_intro as core_start_date

    from 
        install_with_aff
        left join marketing_bucket on install_with_aff.source = marketing_bucket.marketing_source
        left join core_journey on core_journey.user_id=install_with_aff.user_email
),

------ calculation ------
final AS (
    SELECT
        [install_date:aggregation] dt,
        count(appsflyer_id) install,
        count(lite_date) lite,
        count(core_start_date) core_start,
        count(core_date) core,
        count(ffa_with_aff_date) ffa
    FROM
        install_model
    GROUP BY
        1
) 
SELECT
    dt,
    lite,
    core_start,
    core,
    ffa,
   -- (1.00 * core / lite) lite_to_core,
    (1.00 * core_start / lite) lite_to_core_start,
    (1.00 * core / core_start) core_start_to_core,
    (1.00 * ffa / lite) lite_to_ffa
FROM
    final