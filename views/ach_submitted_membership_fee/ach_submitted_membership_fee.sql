--no_cache
(
  select
    userid
    , paymentinfo_feehistories_paymentstatus
    , paymentinfo_feehistories_processor
    , paymentinfo_feehistories_effectivedate
    , paymentinfo_feehistories_returncode
    , paymentinfo_feehistories_amount
  from
    ml_finance.subscription_payment
  where
    paymentinfo_feehistories_processor = 'Payliance'
    and paymentinfo_feehistories_paymentstatus in (
      'Rejected'
      , 'Checked'
      , 'Pending'
    )
      and paymentinfo_feehistories_returncode not in (
    'BV Failure'
    , 'Consecutive NSF'
    , 'Duplicate Transaction ID'
    , 'Exceeded Overdue'
    , 'Hard Return'
    , 'HardReturn'
    , 'Internal Error'
    , 'Invalid Fed ACH'
    , 'MISSED'
    , 'Failed white list check'
    , 'Prior return on routing and account.'
    , 'Transaction previously returned unauthorized'
    , 'Invalid Account Type'
    , '1'
    , '2'
    , '25'
    , '21'
  )
)
union
(
select
  userid
  , paymentinfo_feehistories_paymentstatus
  , paymentinfo_feehistories_processor
  , paymentinfo_feehistories_effectivedate
  , paymentinfo_feehistories_returncode
  , paymentinfo_feehistories_amount
from
  ml_finance.subscription_payment
where
  paymentinfo_feehistories_processor = 'Payliance'
  and paymentinfo_feehistories_paymentstatus = 'Waived'
  and (
    lower(paymentinfo_feehistories_returncode) like 'r__%'
    or lower(paymentinfo_feehistories_returncode) like 'c__%'
  )
  and paymentinfo_feehistories_returncode not in (
      'BV Failure'
      , 'Consecutive NSF'
      , 'Duplicate Transaction ID'
      , 'Exceeded Overdue'
      , 'Hard Return'
      , 'HardReturn'
      , 'Internal Error'
      , 'Invalid Fed ACH'
      , 'MISSED'
      , '1'
      , '2'
    )
)