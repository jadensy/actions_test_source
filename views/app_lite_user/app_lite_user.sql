select 
    a.anonymous_id,
    a.dt,
    a.dtime as su_time,
    a.email,
    a.platform
  from(
    select 
      anonymous_id,
      date("timestamp") as dt,
      "timestamp" as dtime,
      lower(user_id) as email,
      'ios' as platform

    from ios.screen_viewed 
    where true
      and user_id is not null 

    union all

    select 
      anonymous_id,
      date("timestamp") as dt,
      "timestamp" as dtime,
      lower(user_id) as email,
      'android' as platform

    from android.screen_viewed 
    where true
      and user_id is not null 
  )a
  join lion1.user as b
    on b.email = a.email
      and date(b.createdon) = a.dt
  where true
    and b.deleted <> 1
    and b.brand = 'ml'