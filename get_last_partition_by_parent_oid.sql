create or replace function get_last_partition_by_parent_oid(_parent_oid oid)
    returns oid
    language plpgsql stable as
$fnc$
declare
begin
    return (
    select
            c2.oid,
            pg_catalog.pg_get_expr(c2.relpartbound, c2.oid) like '%MAXVALUE%'
        from pg_class c1
            join pg_inherits i on i.inhparent = c1.oid
            join pg_class c2 on c2.oid = i.inhrelid
        where
                c1.oid = _parent_oid
            and pg_catalog.pg_get_expr(c2.relpartbound, c2.oid) like '%MAXVALUE%'
    );
end;
$fnc$;
