/*
 * Tries to acquire share update exclusive lock on a table on given schema
 * (except for tables specified in `_except_tables`)
 * and disables autovacuum when successfull or until a timeout
 * has elapsed.
 * Timeout is essential to avoid long-running transaction.
 * Function should be called in an outer loop to avoid locking the table for too long
 * Functions should be called until its done
 * Function is done when it returns 'done'
 */
create or replace function try_disable_vac
(
    _relnsp         name        default 'materializer',
    _except_tables  name[]      default array['targets'],
    _timeout        interval    default interval'1 minute'
)
    returns text
    language plpgsql volatile as
$fnc$
declare
    _relname name;
    _start_ts timestamptz;
    _cursor refcursor;
begin
    select clock_timestamp() into _start_ts;
    /* find unprocessed tables */

    /* debug:
    raise notice 'started at %', _start_ts;
    */

    loop
        for _relname in
            select relname
                from pg_class c
                where
                        relnamespace='materializer'::regnamespace
                    and relkind = 'r'
                    and relname != 'targets'
                    and not exists
                    (
                        select from pg_options_to_table(c.reloptions)
                            where
                                    option_name = 'autovacuum_enabled'
                                and option_value = 'false'
                    )
                order by ctid
        loop
            /* debug:
            raise notice 'trying rel: %', _relname;
            */

            if ( (clock_timestamp()-_start_ts) > _timeout) then
                return 'timeout';
            end if;

            begin
            /* try to acquire share update exclusive lock on the table */
                execute
                (
                    format('lock table %I.%I in share update exclusive mode nowait;', _relnsp, _relname)
                );
            /* if we weren't able to acquire the lock, just skip the table for now */
            exception
                when lock_not_available then
                    /* debug:
                    raise notice 'failed';
                    */
                    continue;
            end;

            /* debug:
            raise notice 'lock acquired';
            */

            execute
            (
                format
                (
                    'alter table %I.%I set (autovacuum_enabled = false, toast.autovacuum_enabled = false)',
                    _relnsp, _relname
                )
            );
            /* release the lock asap */
            return 'processed';
        end loop;

        if not exists
        (
                select from pg_class c
                    where
                            relnamespace='materializer'::regnamespace
                        and relkind = 'r'
                        and relname != 'targets'
                        and not exists
                        (
                            select from pg_options_to_table(c.reloptions)
                                where
                                        option_name = 'autovacuum_enabled'
                                    and option_value = 'false'
                        )
        ) then
            return 'done';
        end if;

    end loop;
end;
$fnc$;
