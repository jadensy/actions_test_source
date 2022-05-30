/*******************************************************************************************************************************************
Amazon Redshift has three lock modes:

AccessExclusiveLock:      Acquired primarily during DDL operations, such as ALTER TABLE, DROP, or TRUNCATE. 
                          AccessExclusiveLock blocks all other locking attempts.

AccessShareLock:          Acquired during UNLOAD, SELECT, UPDATE, or DELETE operations. AccessShareLock blocks only AccessExclusiveLock attempts. 
                          AccessShareLock doesn't block other sessions that are trying to read or write on the table.

ShareRowExclusiveLock:    Acquired during COPY, INSERT, UPDATE, or DELETE operations. 
                          ShareRowExclusiveLock blocks AccessExclusiveLock and other ShareRowExclusiveLock attempts, but doesn't block AccessShareLock attempts.
*******************************************************************************************************************************************/

--no_cache
select * from admin.v_get_blocking_locks