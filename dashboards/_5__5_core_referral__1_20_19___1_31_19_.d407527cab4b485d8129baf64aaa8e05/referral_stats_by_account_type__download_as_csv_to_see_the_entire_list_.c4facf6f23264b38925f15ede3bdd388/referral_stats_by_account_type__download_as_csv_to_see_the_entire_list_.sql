-- doc type of referrer
WITH base_query as 
(
SELECT email, 
       test_doc_type
FROM 
(
SELECT row_number() OVER (PARTITION by email order by test_doc_type DESC  ) as Rank,
       email,
       test_doc_type
FROM 
(
SELECT distinct pr.referreremail as email,
             CASE WHEN s.doc_type IS NOT NULL THEN s.doc_type ELSE 'Lite' End as test_doc_type
FROM ml_public.usercollection us
INNER JOIN ml_public.referral pr  on us.email= pr.referreremail
LEFT JOIN ml_public.subscription s ON s.userid = us.userid
  )
  ) where Rank =1
  ),



-- connecting referrer to referee 
super_base as 
(
SELECT base_query.*,
       rf.refereeemail as refree
FROM  ml_public.referral rf 
INNER JOIN base_query ON base_query.email=rf.referreremail
  where [rf.referralevent_createdon=daterange]
  ),


-- querying referee's doctype 

super_base_3 as 
(
SELECT email,
      CASE WHEN  referer_doc_type NOT IN ('ML-Plus','ML-Free-Heavy') OR referer_doc_type is null THEN 'Lite' ELSE referer_doc_type END as referer_doc_type ,
refree, 
 CASE WHEN  doc_type NOT IN ('ML-Plus','ML-Free-Heavy') or doc_type is null  THEN 'Lite' ELSE doc_type END as doc_type 
FROM 
(
SELECT DISTINCT super_base.email,
       super_base.test_doc_type as referer_doc_type,
       super_base.refree,
       s.doc_type
FROM   ml_public.usercollection us
INNER JOIN super_base ON super_base.refree = us.email
LEFT JOIN ml_public.subscription s ON s.userid= us.userid
  )
  )




SELECT email as referrer_email, 
       referer_doc_type,
       --refree as "User who got referrerd", 
       SUM(case when doc_type ='Lite' Then 1 End) as Lite_count,
       SUM(case when doc_type ='ML-Free-Heavy' Then 1 End) as core_count,
       SUM(case when doc_type ='ML-Plus' Then 1 End) as plus_count
FROM super_base_3
GROUP By 1,2