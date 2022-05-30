with all_withdrawals as (
  select 
         drivewealthaccount___id as _id, 
         _id as withdrawal_id, 
         max(createdon) as withdrawal_date
  from wealth_raw.drivewealthaccount__withdrawals
  where createdon >= '2018-10-01'
  group by 1,2
),
all_withdrawals_with_userid as (
  select *
  from all_withdrawals
    left join 
    (
      select
        users.userid as userid,
        acct_user.drivewealthaccountid
      from wealth_raw.drivewealthuser as users
        left join wealth_raw.drivewealthaccountuser acct_user
        on users._id = acct_user.drivewealthuserid
    ) user_identifier
    on all_withdrawals._id = user_identifier.drivewealthaccountid
)

select a.*, 
       b.status, 
       b.amount, 
       b.type, 
       b.eventtype, 
       b.feeamount, 
       b.createdon as withdrawal_createdon, 
       b.beneficiaryaccountnumber, 
       b.beneficiaryroutingnumber, 
       b.donereversedcashtransfer, 
       b.transactionid, 
       b.transactionidstatus
from all_withdrawals_with_userid as a
  left join wealth_raw.drivewealthaccount__withdrawals as b
  on (a.withdrawal_date = b.createdon and a.withdrawal_id = b._id)