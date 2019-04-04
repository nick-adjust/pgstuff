create or replace function johnny_walker(_load int4 default 1)
    returns setof pg_class
    language plpgsql as
$f$
declare
    _shit refcursor;
    _ctid text;
    _coming int4;
begin
    _shit = '_shit';
    _coming = 1;
--    if not exists(select from pg_cursors where name = '_shit') then
    open _shit for select ctid::text from pg_class;
--    end if;
    while _coming = 1 loop
        execute(format('fetch %s from %I',_load, _shit)) into _ctid;

        get diagnostics _coming = row_count;
        raise notice '% I walk along this lonely %', _coming, _ctid;
    end loop;
    return;
end;
$f$;
