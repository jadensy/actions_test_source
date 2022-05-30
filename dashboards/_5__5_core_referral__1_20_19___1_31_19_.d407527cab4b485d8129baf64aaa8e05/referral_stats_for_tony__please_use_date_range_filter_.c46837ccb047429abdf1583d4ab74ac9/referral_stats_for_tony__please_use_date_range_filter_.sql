with current_doc_type as 
(
SELECT DISTINCT us.email,
       s.doc_type
  FROM ml_public.usercollection us
  INNER JOIN ml_public.subscription s on us.userid = s.userid
  where us.brand='ml'
  ),


-- referrer doctype

referrer as 
(
SELECT DISTINCT pr.referreremail,
       CASE WHEN current_doc_type.doc_type IN ('ML-Plus','ML-Free-Heavy') then current_doc_type.doc_type else 'Lite' END as referrer_doc_type

FROM ml_public.referral pr 
LEFT JOIN current_doc_type on current_doc_type.email = pr.referreremail
  where [pr.createdon =daterange]
  and pr.createdon  AT TIME ZONE 'UTC' AT TIME ZONE 'EST' BETWEEN '2019-01-20 00:00:00' and '2019-01-31 11:59:59'

--where pr.createdon BETWEEN '2019-01-01' AND '2019-01-31'
),

referee as (

SELECT DISTINCT  prr.refereeemail,
        prr.referreremail,
       CASE WHEN current_doc_type.doc_type IN ('ML-Plus','ML-Free-Heavy') then current_doc_type.doc_type else 'Lite' END as referee_doc_type,
       CASE WHEN length(ac.user_id) > 0 then 1 else 0 end as org_core

FROM ml_public.referral prr 
LEFT JOIN current_doc_type on current_doc_type.email = prr.referreremail
LEFT JOIN prod.core_account_created ac on ac.user_id= prr.refereeemail
  where [prr.createdon =daterange] 
  and [ac.timestamp=daterange]
   and prr.createdon  AT TIME ZONE 'UTC' AT TIME ZONE 'EST' BETWEEN '2019-01-20 00:00:00' and '2019-01-31 11:59:59'
   and ac.timestamp  AT TIME ZONE 'UTC' AT TIME ZONE 'EST' BETWEEN '2019-01-20 00:00:00' and '2019-01-31 11:59:59'
-- where prr.createdon BETWEEN '2019-01-01' AND '2019-01-31'
--   and ac.timestamp BETWEEN '2019-01-01' AND '2019-01-31'
and prr.referreremail in (SELECT referreremail FROM referrer )
  )




SELECT referreremail,
       sum(ml_plus) as ml_plus,
       sum(current_core) as current_core,
       sum(Lite) as Lite,
       sum(org_core) as org_core
FROM 
(

SELECT referee.*,
       referrer.referrer_doc_type,
       case when referee.referee_doc_type ='ML-Plus' THEN 1 ELSE 0 END as ml_plus,
        case when referee.referee_doc_type ='ML-Free-Heavy' THEN 1 ELSE 0 END as current_core,
         case when referee.referee_doc_type ='Lite' THEN 1 ELSE 0 END as Lite
FROM referee
INNER JOIN referrer on referrer.referreremail = referee.referreremail
  )
GROUP BY 1