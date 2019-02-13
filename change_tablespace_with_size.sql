create or replace function change_tablespace_with_size
(
    _src_schema name,
    _src_spc    name,
    _dst_spc    name,
    _size       int8 default int8'2'*1024*1024*1024*1024 /* number of bytes to move */
)
    returns int8 /* number of bytes actually moved */
    language plpgsql volatile as
$fnc$
declare
    _relnamespace   oid;
    _relname        name;
    _relsize        int8;
    _result         int8:=0;
    _relspcoid_src  int4; /* this is int4, because it can be 0 which is not very nice for an oid */
    _ddl            text;
    _err_sqlstate   text;
    _err_message    text;
    _err_detail       text;
begin
    /* Get source tablespace id for pg_class. pg_default is always 0
     * Throws an error if there is no such tablespace (INTO STRICT)
     */
    select
            case _src_spc
                when 'pg_default' then 0
                else oid
            end
                into strict _relspcoid_src
        from pg_tablespace
        where spcname = _src_spc;

    /* Check if destination tablespace exists and throw an error if it doesn't */
    if not exists(select from pg_tablespace where spcname = _dst_spc) then
        raise exception 'Tablespace % does not exist', _dst_spc;
    end if;

    /* check if source schema exists and error out if it doesn't */
    select oid into strict _relnamespace from pg_namespace where nspname = _src_schema;

    /* Loop through relations (indexes and tables) in source tablespace until
     * size threshold is reached and run ddl to move relations from source
     * tablespace to destination.
     * To determine where to stop a window function is used.
     *
     * In this case postgres doesn't differentiate between indexes and tables
     * since both ALTER INDEX and ALTER TABLE share the same bison rule and
     * cases when ALTER INDEX is not applicable are filtered during runtime.
     * So its safe to run ALTER TABLE .. SET TABLESPACE on an index.
     *
     * When the source tablespace is pg_default its id in pg_class is 0
     * as it is for all the catalog entries, thus specifying the schema is
     * a strict requirement, since we probably don't want catalog objects
     * to be moved.
     */
    for _relname, _relsize, _ddl in
    (
        select
                relname,
                relsize,
                format('ALTER TABLE %I.%I SET TABLESPACE %I;', _src_schema, relname, _dst_spc)
            from
            (
                select
                        relname,
                        relsize,
                        relkind,
                        reldate,
                        sum(relsize) over (order by reldate asc)  running_sum
                    from
                    (
                        select
                                relname,
                                relkind,
                                pg_relation_size(c.oid) relsize,
                                substring(relname::text,'_(\d+_\d+_\d+)')::date reldate
                            from pg_class c
                            where
                                    true
                                and c.relkind = any (array['r', 'i'])
                                and c.reltablespace = _relspcoid_src
                                and relnamespace = _relnamespace
                    ) rels
            ) a
        where running_sum <= _size
        order by reldate asc
    ) loop
        raise notice 'About to process %.%',
            quote_ident(_src_schema), quote_ident(_relname);
        _result = _result + _relsize;
        /*
         * We run the actual ddl inside a subtransaction. This way a failure in
         * a single statement does not result in a rollback and we can carry on
         * with the rest of tables.
         */
        begin
            execute _ddl;
        exception when others then
            get stacked diagnostics _err_sqlstate = RETURNED_SQLSTATE;
            get stacked diagnostics _err_message = MESSAGE_TEXT;
            get stacked diagnostics _err_detail = PG_EXCEPTION_DETAIL;
            raise warning 'Failed to move %.%. SQLSTATE %',
                quote_ident(_src_schema), quote_ident(_relname), _err_sqlstate
                using
                    detail = format('%sMessage: "%s" %sDetail: "%s"',
                                     E'\n\t', _err_message, E'\n\t', _err_detail);
            continue; /* move to the next item without outputting "Done" message */

        end;

        raise notice 'Done with %.%',
            quote_ident(_src_schema), quote_ident(_relname);
    end loop;
    return _result;
end;
$fnc$;

/*
select * from change_tablespace_with_size('public', 'pg_default', 'spc');
*/
