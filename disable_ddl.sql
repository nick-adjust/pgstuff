-- drop function maintenance.disable_ddl(regclass[],boolean,boolean,boolean,boolean,boolean,boolean);
create or replace function maintenance.disable_ddl
(
    _table_list     regclass[],                     -- Array of tables
    _disable_fk     bool default true,              -- Disable Foreign Key Constraints ?
    _disable_fk_ex  bool default true,              -- Disable any external Foreign Keys (referring to objects in _table_list)?
    _disable_idx    bool default true,              -- Disable Indexes? (this only drops indexes that have no constraints depending on them)
    _disable_ic     bool default true,              -- Disable Indexed Constraints? (these include PKeys, Unique constraints, Exclude constraints)
    _disable_tg     bool default true,              -- Disable user Triggers?
    _exit_on_error  bool default false,             -- Exit on first error
out __batch         text,                           -- Batch ID used to recreate dropped objects
out __num_err       int8                            -- Number of errors (objects)
)
    returns record
language plpgsql volatile as
$do$
declare
    _q      text;
    _target name;
    _rel    name;
    _nsp    name;
    _e_m    text;
    _e_d    text;
    _e_h    text;
begin
    -- settings search path to safely use pg_get_constraintdef()
    perform set_config('search_path', 'maintenance', true);
    select md5(_table_list::text) into __batch;
    -- create finalize table if not yet present. This table will be used to recreate dropped objects
    create table if not exists maintenance.rel_disabled_ddl
    (
        ord         serial,             -- order is stored to recreate in correct order
        batch       text,               -- batch id used to identify related records
        nsp         name not null,      -- schema
        rel         name not null,      -- table
        target      name not null,      -- target (constraint, index or trigger name)
        state       text,               -- target state
        err         text,               -- error messages (appended each time there is an error, cleaned when processed succesfully)
        def         text,               -- DDL to create target
        undef       text,               -- DDL to drop target
        size        int8,               -- size in bytes. Might be useful when restoring in parallel
            unique(nsp, rel, target)
    );

    -- Foreign keys referring to tables in _table_list
    if (_disable_fk_ex) then
        insert into maintenance.rel_disabled_ddl(nsp, rel, target, def, undef,state, batch, size)
            select
                    nt.nspname,
                    ct.relname,
                    con.conname,
                    format('alter table %I.%I add constraint %I %s', nt.nspname, ct.relname, con.conname, pg_get_constraintdef(con.oid)),
                    format('alter table %I.%I drop constraint %I', nt.nspname, ct.relname, con.conname),
                    'remove pending',
                    __batch,
                    0
                from
                    pg_constraint con
                        left join pg_class ct on ct.oid=con.conrelid
                        left join pg_namespace nt on nt.oid=ct.relnamespace
                where
                        contype = 'f'
                    and confrelid  = any (_table_list)
            on conflict (nsp, rel, target) do
                update
                    set
                        ord=excluded.ord,
                        def=excluded.def,
                        undef=excluded.undef,
                        state=excluded.state;
    end if;

    -- Foreign keys:
    if (_disable_fk) then
        insert into maintenance.rel_disabled_ddl(nsp, rel, target, def, undef,state, batch, size)
            select
                    nt.nspname,
                    ct.relname,
                    con.conname,
                    format('alter table %I.%I add constraint %I %s', nt.nspname, ct.relname, con.conname, pg_get_constraintdef(con.oid)),
                    format('alter table %I.%I drop constraint %I', nt.nspname, ct.relname, con.conname),
                    'remove pending',
                    __batch,
                    0
                from
                    pg_constraint con
                        left join pg_class ct on ct.oid=con.conrelid
                        left join pg_namespace nt on nt.oid=ct.relnamespace
                where
                        contype = 'f'
                    and conrelid  = any (_table_list)
            on conflict (nsp, rel, target) do
                update
                    set
                        ord=excluded.ord,
                        def=excluded.def,
                        undef=excluded.undef,
                        state=excluded.state;
    end if;

    -- Index-dependant Constraints (to remove indexes they rely on). These include PKeys (p), Unique (u) constraints and exclude (x) constraints
    if(_disable_ic) then
        insert into maintenance.rel_disabled_ddl(nsp, rel, target, def, undef,state, batch, size)
            select
                    nt.nspname,
                    ct.relname,
                    con.conname,
                    format('alter table %I.%I add constraint %I %s %s',nt.nspname, ct.relname, con.conname, pg_get_constraintdef(con.oid), 'using index tablespace '|| quote_ident(spcname)),
                    format('alter table %I.%I drop constraint %I',nt.nspname, ct.relname, con.conname),
                    'remove pending',
                    __batch,
                    pg_relation_size(i.indexrelid)
                from
                    pg_constraint con
                        left join pg_class ct on ct.oid=con.conrelid
                        left join pg_namespace nt on nt.oid=ct.relnamespace
                        left join pg_index i on con.conindid=i.indexrelid
                        left join pg_class ci on i.indexrelid=ci.oid
                        left join pg_tablespace t on ci.reltablespace=t.oid
                where
                        contype = any (array['u', 'p', 'x'])
                    and conrelid = any (_table_list)
            on conflict (nsp, rel, target) do
                update
                    set
                        ord=excluded.ord,
                        def=excluded.def,
                        undef=excluded.undef,
                        state=excluded.state;
    end if;


    -- Indexes
    if (_disable_idx) then
        insert into maintenance.rel_disabled_ddl(nsp, rel, target, def, undef,state, batch, size)
            select
                    nt.nspname,
                    ct.relname,
                    ci.relname,
                    format('%s %s ', pg_get_indexdef(i.indexrelid), 'tablespace '||quote_ident(spcname)),
                    format('drop index %I.%I', ni.nspname, ci.relname),
                    'remove pending',
                    __batch,
                    pg_relation_size(i.indexrelid)
                from
                    pg_index i
                        left join pg_class ct on ct.oid=i.indrelid
                        left join pg_namespace nt on nt.oid=ct.relnamespace
                        left join pg_class ci on i.indexrelid=ci.oid
                        left join pg_namespace ni on ni.oid=ci.relnamespace
                        left join pg_tablespace t on ci.reltablespace=t.oid
                where
                        true
                    and  not exists(select from pg_constraint con where con.conindid=i.indexrelid) -- exclude constraint indexes
                    and indrelid = any (_table_list)
            on conflict (nsp, rel, target) do
                update
                    set
                        ord=excluded.ord,
                        def=excluded.def,
                        undef=excluded.undef,
                        state=excluded.state;
    end if;

    -- Triggers
    if(_disable_tg) then
        insert into maintenance.rel_disabled_ddl(nsp, rel, target, def, undef,state, batch, size)
            select
                    nt.nspname,
                    ct.relname,
                    t.tgname,
                    pg_get_triggerdef(t.oid, true),
                    format('drop trigger %I on %I.%I', t.tgname, nt.nspname, ct.relname),
                    'remove pending',
                    __batch,
                    0
                from pg_trigger t
                    left join pg_class ct on ct.oid = t.tgrelid
                    left join pg_namespace nt on nt.oid=ct.relnamespace
                where
                        t.tgrelid = any (_table_list)
                    and not tgisinternal
            on conflict (nsp, rel, target) do
                update
                    set
                        ord=excluded.ord,
                        def=excluded.def,
                        undef=excluded.undef,
                        state=excluded.state;
    end if;

    -- Run prepared DDLs and update finalize-table states
    set client_min_messages='notice';
    for _q, _target, _rel, _nsp in select undef, target, rel, nsp from maintenance.rel_disabled_ddl where batch=__batch and state='remove pending' order by ord loop
        raise notice 'dropping %.%.% with: %', _nsp, _rel, _target, _q;
        begin
            execute(_q);

            update maintenance.rel_disabled_ddl
                set state='removed'
                where
                        nsp=_nsp
                    and rel=_rel
                    and target=_target;
            -- each loop iteration has to be in its own sub-transaction since we don't want to lose progress if a failure occurs
        exception
            when others then
                get stacked diagnostics
                    _e_m = message_text,
                    _e_d = pg_exception_detail,
                    _e_h = pg_exception_hint;
                -- update state if failed
                update maintenance.rel_disabled_ddl
                    set
                        state='failed',
                        err=coalesce(err, '') || format(E'Message: %s\nDetail: %s\nHint:%s\n', _e_m, _e_d, _e_h)
                    where
                            nsp=_nsp
                        and rel=_rel
                        and target=_target;
                if (_exit_on_error) then
                    exit;
                end if;
        end;
    end loop;

    select count(*) into __num_err from maintenance.rel_disabled_ddl where batch=__batch and state = 'failed';
    return;
end;
$do$;
