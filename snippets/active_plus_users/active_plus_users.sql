select userid
  from ml_public.subscription
  where ml_public.subscription.status = 'active'
  and ml_public.subscription.doc_type = 'ML-Plus'
  and ml_public.subscription.subtype != 'ReturningOOS'