with 
source as
(
  select
    te.*,
    fo.type as source_type,
    f.type as destination_type,
    row_number() over(partition by transaction_id order by te.created_at) as asc_rn,
    row_number() over(partition by transaction_id order by te.created_at desc) as desc_rn
  from ml_public.postgres_fund_transfer_event te
  left join ml_public.postgres_fund_option fo on
    te.source_id = fo.id
  left join ml_public.postgres_fund_option f on
    te.destination_id = f.id
  where f.type is not null
    and fo.type is not null
)

select
  sum(amount) as "Total $ Amount",
  count(distinct transaction_id) as total_number_of_transactions
from
(
  select
    transaction_id,
    transaction_date,
    user_id,
    amount,
    source_type as source,
    destination_type as destination,
    state,
    description
  from source
  where
    source_type = 'DEBIT_CARD' and
    destination_type = 'MONEYLION_COREPRO_ACCOUNT' and
    state = 'REQUEST' and
    [transaction_date:pst:day] >= '2019-02-22' and
    [transaction_date:pst:hour] >= dateadd(hour,-24,[getdate():pst:hour])
  order by transaction_date desc
)