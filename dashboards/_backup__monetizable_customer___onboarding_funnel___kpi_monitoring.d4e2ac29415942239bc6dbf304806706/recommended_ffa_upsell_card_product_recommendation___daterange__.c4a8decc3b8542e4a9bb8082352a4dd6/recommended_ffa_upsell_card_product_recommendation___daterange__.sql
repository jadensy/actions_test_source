/*
MAIN: LITE 2.0 JOURNEY ANALYSIS
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

------ today card generated ------
today_card_status as (
    select
        "timestamp" :: date card_date,
        "timestamp" card_datetime,
        user_id as user_email,
        index,
        element,
        product,
        element_key,
        ------ transformation ------
        case
            when element = 'UpsellCard' then 1
        end is_upsell,
        case
            when element_key in (
                ------ roarmoney ------
                'RoarMoneyUpsellCard',
                'RoarMoneyUpsellCardDay0',
                ------ instacash ------
                'InstaCashEligibles',
                'InstaCashEligiblesDay0',
                ------ crypto ------
                'CryptoTradeBonusCard',
                'CryptoTradeBonusCardDay0',
                ------ investment ------
                'WealthAutoInvestNoInv',
                'WealthAutoInvestNoInvDay0',
                ------ cbplus ------
                'CreditBuilderLockedCard',
                'CreditBuilderLockedCardA',
                'CreditBuilderLockedCardB',
                'CreditBuilderLockedCardC',
                'CreditBuilderLockedCardD',
                'CreditBuilderLockedCardDay0'
            ) then 1
        end is_upsell_d0
    from
        prod.today_card_status
        cross join date_select
    where
        card_date >= date_select.start_date
        and card_date <= date_select.end_date
),

------ today recommendation ------

today_recommendation as (
    select 
        gp_selection_focus_area.email,
        gp_selection_focus_area.lite_timestamp,
        gp_selection_focus_area.screen_timestamp,
        
        today_card_status.card_date,
        today_card_status.card_datetime,
        today_card_status.index :: int card_index,
        today_card_status.element card_element,
        today_card_status.product card_product_tag,
        today_card_status.element_key card_element_key,
        today_card_status.is_upsell,
        today_card_status.is_upsell_d0,
        
        ------ transformation ------
        case
            when today_card_status.element_key in (
                'CreditBuilderLockedCard',
                'CreditBuilderLockedCardA',
                'CreditBuilderLockedCardB',
                'CreditBuilderLockedCardC',
                'CreditBuilderLockedCardD',
                'CreditBuilderLockedCardDay0'
            ) then 'cbplus'
            when today_card_status.element_key in (
                'InstaCashEligibles',
                'InstaCashEligiblesDay0'
            ) then 'instacash'
            when today_card_status.element_key in (
                'RoarMoneyUpsellCard',
                'RoarMoneyUpsellCardDay0'
             ) then 'roar money'
            when today_card_status.element_key in (
                'WealthAutoInvestNoInv',
                'WealthAutoInvestNoInvDay0'
             ) then 'investment standalone'
            when today_card_status.element_key in  (
                'CryptoTradeBonusCard',
                'CryptoTradeBonusCardDay0'
            ) then 'crypto'
            else 'others'
        end card_product,
        
        row_number() over(
            partition by email,
                card_date
            order by 
                card_datetime,
                card_index
        ) rn_card
    from
        gp_selection_focus_area
        left join today_card_status on (
            gp_selection_focus_area.email = today_card_status.user_email
            and gp_selection_focus_area.screen_timestamp :: date = today_card_status.card_date
        )
),

------ calculation ------
calculation as (
    select
        card_product,
        count(card_datetime) card_count
    from
        today_recommendation
    where
        rn_card = 1 
        and card_index = 1
    group by
        1
),

total as (
    select 
        count(card_datetime) total
    from 
        today_recommendation
    where
        rn_card = 1
        and card_index = 1
)

select
    card_product,
    card_count,
    (1.00 * card_count / total) card_pct
from
    calculation 
    cross join total
order by
    2 desc
limit
    200