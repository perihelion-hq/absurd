-- Migration from 0.5.0 to main
--
-- Brings a 0.5.0 database to the current sql/absurd.sql:
--
-- * Adds per-queue claim functions (absurd.ct_<queue>) so claim_task() runs
--   statically planned, plan-cached SQL instead of re-planning dynamic SQL on
--   every poll.  claim_task() keeps its signature and result columns and falls
--   back to its previous dynamic statements for any queue without a generated
--   function (or for a caller without EXECUTE on it).
-- * Tightens set_task_checkpoint_state() and await_event() to verify that the
--   run belongs to the given task, and makes await_event() prefer a timeout
--   wake-up over a later event.
-- * Makes extend_claim() and set_task_checkpoint_state() renew a claim from
--   when the renewal is written, not from before a row-lock wait.
-- * Serializes cleanup_tasks() and cleanup_events() with concurrent work
--   (row locks, skip locked; cleanup_tasks() locks runs before their task)
--   and rejects a cleanup limit below 1, and makes await_event() keep an event
--   row locked even when cleanup deletes the row it found.
-- * Lowers the queue-name limit from 57 to 53 bytes (validate_queue_name) and
--   makes weekly partition tags year-unique (partition_week_tag).
--
-- RESTRICTION: every existing queue name must be at most 53 bytes.  0.5.0
-- accepted 54 to 57 bytes, but a year-unique weekly partition name
-- ('t_' || queue || '_' || IYYYIW) of such a queue would exceed PostgreSQL's
-- 63-byte identifier limit and be truncated, and main rejects such names in
-- create_queue(), get_task_result() and the other functions that validate the
-- queue name.  The first statement below therefore aborts the whole migration,
-- before anything is changed, and lists the affected queues.  To remediate, on
-- the 0.5.0 database drain each listed queue, drop it with absurd.drop_queue()
-- and recreate it under a name of at most 53 bytes, then apply this migration.
--
-- RESTRICTION: no weekly partition may be renamed to a name another relation
-- already has.  A queue named like a partitioned queue followed by _ and six
-- digits (for example "jobs_202641" beside a partitioned "jobs") owns the
-- partitioned queue's new partition names.  The second statement aborts the
-- migration, before anything is changed, and lists the colliding renames.  To
-- remediate, drain the queue that owns the existing relation, drop it with
-- absurd.drop_queue() and recreate it under another name, then apply this
-- migration.
--
-- No row data is rewritten.  Function definitions are replaced, claim
-- functions are created, and the existing weekly partitions of partitioned
-- queues (and the indexes named after them) are renamed to the new tags,
-- under an ACCESS EXCLUSIVE lock on absurd.queues that waits for concurrent
-- queue and partition maintenance and briefly blocks reads of that table.
-- This file contains no BEGIN/COMMIT.  Apply it as one transaction (for
-- example psql -v ON_ERROR_STOP=1 -1 -f ...) so a failure leaves the 0.5.0
-- schema untouched.  With ON_ERROR_STOP alone (as absurdctl migrate applies
-- it) psql stops at the first error but keeps the statements that already
-- ran.  The two preflights below run first and change nothing either way, the
-- partition tag function and the partition renames change together in one
-- statement, and the new schema version is published by the last statement,
-- so after a failure the database still reports 0.5.0 and the next migrate
-- applies this file again.  Safe to re-run.

do $$
declare
  v_queues text;
begin
  select string_agg(
           format('"%s" (%s bytes, %s)', queue_name, octet_length(queue_name), storage_mode),
           ', ' order by queue_name)
    into v_queues
    from absurd.queues
   where octet_length(queue_name) > 53;

  if v_queues is not null then
    raise exception 'Cannot migrate to main: queue names are limited to 53 bytes, but these queues are longer: %', v_queues
      using hint = 'Drain each listed queue, drop it with absurd.drop_queue() and recreate it under a name of at most 53 bytes, then apply this migration again.  Nothing has been changed.',
            errcode = 'check_violation';
  end if;
end;
$$;

-- The weekly partition renames further below must not collide with another
-- relation.  A partitioned queue's new partition name can be a valid 0.5.0
-- queue's table name: partition t_jobs_641 of queue "jobs" becomes
-- t_jobs_202641, the task table of a queue "jobs_202641".  This computes every
-- rename the way the rename statement does (with the new partition_week_tag()
-- definition) and aborts, before anything is changed, if a target name is
-- taken or targeted twice.
do $$
declare
  v_collisions text;
begin
  with partitions as (
    select child.oid as child_oid,
           child.relname as child_name,
           parent.relname as parent_name,
           substring(
             pg_catalog.pg_get_expr(child.relpartbound, child.oid)
             from 'FROM \(''([0-9a-f-]{36})'''
           )::uuid as lower_bound
      from absurd.queues q
      join pg_catalog.pg_class parent
        on parent.relnamespace = 'absurd'::regnamespace
       and parent.relname in ('t_' || q.queue_name, 'r_' || q.queue_name,
                              'c_' || q.queue_name, 'w_' || q.queue_name)
      join pg_catalog.pg_inherits i on i.inhparent = parent.oid
      join pg_catalog.pg_class child on child.oid = i.inhrelid
     where q.storage_mode = 'partitioned'
       and substr(child.relname, length(parent.relname) + 1) ~ '^_[0-9]{3}$'
  ),
  tables as (
    select p.child_oid,
           p.child_name,
           p.parent_name || '_' ||
             (extract(isoyear from w.bucket)::int)::text ||
             lpad((extract(week from w.bucket)::int)::text, 2, '0') as new_name
      from partitions p
     cross join lateral (
       select absurd.week_bucket_utc(to_timestamp(
                ('x' || substr(replace(p.lower_bound::text, '-', ''), 1, 12))::bit(48)::bigint
                / 1000.0
              )) at time zone 'UTC' as bucket
     ) w
     where p.lower_bound is not null
  ),
  renames as (
    select t.child_name as old_name, t.new_name
      from tables t
    union all
    select ic.relname,
           t.new_name || substr(ic.relname, length(t.child_name) + 1)
      from tables t
      join pg_catalog.pg_index ix on ix.indrelid = t.child_oid
      join pg_catalog.pg_class ic on ic.oid = ix.indexrelid
     where left(ic.relname, length(t.child_name) + 1) = t.child_name || '_'
       and octet_length(t.new_name)
           + octet_length(ic.relname) - octet_length(t.child_name) <= 63
  )
  select string_agg(format('%s -> %s', r.old_name, r.new_name), ', '
                    order by r.new_name, r.old_name)
    into v_collisions
    from renames r
   where exists (
           select 1
             from pg_catalog.pg_class c
            where c.relnamespace = 'absurd'::regnamespace
              and c.relname = r.new_name
         )
      or (select count(*) from renames d where d.new_name = r.new_name) > 1;

  if v_collisions is not null then
    raise exception 'Cannot migrate to main: these weekly partition renames collide with an existing relation or each other: %', v_collisions
      using hint = 'A queue named like a partitioned queue followed by _ and six digits owns the new partition names of that queue.  Drain the queue that owns the existing relation, drop it with absurd.drop_queue() and recreate it under another name, then apply this migration again.  Nothing has been changed.',
            errcode = 'duplicate_table';
  end if;
end;
$$;

-- Queue names are used in generated table/index identifiers.
-- We intentionally cap UTF-8 byte length so generated weekly partition names
-- (<prefix>_<queue>_<ISO year/week>) stay within PostgreSQL's 63-byte identifier
-- limit. Character set is otherwise delegated to PostgreSQL quoted-ident rules.
create or replace function absurd.validate_queue_name (p_queue_name text)
  returns text
  language plpgsql
as $$
begin
  if p_queue_name is null or p_queue_name = '' then
    raise exception 'Queue name must be provided';
  end if;

  if octet_length(p_queue_name) > 53 then
    raise exception 'Queue name "%" is too long (max 53 bytes).', p_queue_name;
  end if;

  return p_queue_name;
end;
$$;

-- Creates (or replaces) the per-queue claim function absurd.ct_<queue>.
--
-- claim_task() used to run its three statements through EXECUTE, which
-- parses and plans them on every call (dynamic SQL is never cached).  Workers
-- poll claim_task constantly and most polls find nothing, so planning
-- dominated the cost.  The generated function has the same statements as
-- static SQL against this queue's tables, so PL/pgSQL caches the plans per
-- session.  absurd.claim_task() dispatches to it.
--
-- The function is SECURITY INVOKER like every other Absurd function and takes
-- the same arguments as claim_task() except the queue name, which is baked in
-- so it cannot be pointed at another queue.  The queue name must pass
-- validate_queue_name(), which keeps 'ct_' || queue within PostgreSQL's 63-byte
-- identifier limit, so the name is never truncated.
--
-- The function is owned by the owner of the queue's tables, whichever role
-- runs this (a migration role, a superuser, or the queue owner itself), so the
-- queue owner can always replace it (create_queue) and drop it (drop_queue).
-- If the caller cannot create the function or hand it to that owner, no
-- function is created and claim_task() keeps using its dynamic statements.
--
-- Being a separate function, it has its own EXECUTE privilege: claim_task()
-- only dispatches to it for a caller that holds EXECUTE on it, so installations
-- that revoke PUBLIC EXECUTE keep working through the dynamic path until the
-- worker role is granted EXECUTE on absurd.ct_<queue>(text, integer, integer).
--
-- Concurrent calls for one queue are serialized on the queue's row in
-- absurd.queues, which drop_queue() deletes first: PostgreSQL does not
-- serialize two CREATE OR REPLACE FUNCTION statements for the same function
-- (the second fails with a unique violation or "tuple concurrently updated").
-- Locking that row needs UPDATE on absurd.queues; a caller without it gets no
-- function, as for any other privilege it lacks.
create or replace function absurd.ensure_claim_function (p_queue_name text)
  returns void
  language plpgsql
as $$
declare
  v_fn_name text := 'ct_' || p_queue_name;
  v_queue_locked boolean;
  v_table_owner oid;
  v_fn_owner oid;
  v_body text;
begin
  perform absurd.validate_queue_name(p_queue_name);

  -- Everything below reads the catalog only after this lock, so a call that
  -- waited for another one sees what that one committed.
  begin
    select true into v_queue_locked
      from absurd.queues q
     where q.queue_name = p_queue_name
       for no key update;
  exception
    when insufficient_privilege then
      raise notice 'Not creating claim function absurd.%: %', v_fn_name, sqlerrm;
      return;
  end;

  if v_queue_locked is null then
    return;
  end if;

  -- The function must belong to whoever owns the queue's tables, so that role
  -- (and not the role that happened to run this) can later replace or drop it.
  select c.relowner into v_table_owner
    from pg_catalog.pg_class c
   where c.relnamespace = 'absurd'::regnamespace
     and c.relname = 't_' || p_queue_name;

  if v_table_owner is null then
    return;
  end if;

  -- The body goes into the CREATE FUNCTION statement as a quoted literal
  -- (%2$L below), never inside a dollar-quote delimiter, so no queue name can
  -- terminate it early.
  v_body := format(
    $body$
#variable_conflict use_column
declare
  v_queue_name constant text := %4$L;
  v_now timestamptz := absurd.current_time();
  v_claim_timeout integer := greatest(coalesce(p_claim_timeout, 30), 0);
  v_worker_id text := coalesce(nullif(p_worker_id, ''), 'worker');
  v_qty integer := greatest(coalesce(p_qty, 1), 1);
  v_claim_until timestamptz := null;
  v_expired_run record;
  v_cancel_candidate record;
  v_expired_sweep_limit integer;
begin
  if v_claim_timeout > 0 then
    v_claim_until := v_now + make_interval(secs => v_claim_timeout);
  end if;

  -- Keep claim polling work bounded: process at most v_qty expired leases
  -- per claim call.
  v_expired_sweep_limit := greatest(v_qty, 1);

  -- Apply cancellation rules before claiming.
  --
  -- Use cancel_task() so lock order stays consistent (runs first, task second)
  -- with complete_run()/fail_run().
  for v_cancel_candidate in
    select t.task_id
      from absurd.%2$I t
     where t.state in ('pending', 'sleeping', 'running')
       and (
         (
           (t.cancellation->>'max_delay')::bigint is not null
           and t.first_started_at is null
           and extract(epoch from (v_now - t.enqueue_at)) >= (t.cancellation->>'max_delay')::bigint
         )
         or
         (
           (t.cancellation->>'max_duration')::bigint is not null
           and t.first_started_at is not null
           and extract(epoch from (v_now - t.first_started_at)) >= (t.cancellation->>'max_duration')::bigint
         )
       )
     order by t.task_id
  loop
    perform absurd.cancel_task(v_queue_name, v_cancel_candidate.task_id);
  end loop;

  for v_expired_run in
    select r.run_id,
           r.claimed_by,
           r.claim_expires_at,
           r.attempt
      from absurd.%1$I r
     where r.state = 'running'
       and r.claim_expires_at is not null
       and r.claim_expires_at <= v_now
     order by r.claim_expires_at, r.run_id
     limit v_expired_sweep_limit
       for update skip locked
  loop
    perform absurd.fail_run(
      v_queue_name,
      v_expired_run.run_id,
      jsonb_strip_nulls(jsonb_build_object(
        'name', '$ClaimTimeout',
        'message', 'worker did not finish task within claim interval',
        'workerId', v_expired_run.claimed_by,
        'claimExpiredAt', v_expired_run.claim_expires_at,
        'attempt', v_expired_run.attempt
      )),
      null
    );
  end loop;

  return query
  with candidate as (
    select r.run_id
      from absurd.%1$I r
      join absurd.%2$I t on t.task_id = r.task_id
     where r.state in ('pending', 'sleeping')
       and t.state in ('pending', 'sleeping', 'running')
       and r.available_at <= v_now
     order by r.available_at, r.run_id
     limit v_qty
       for update skip locked
  ),
  updated as (
    update absurd.%1$I r
       set state = 'running',
           claimed_by = v_worker_id,
           claim_expires_at = v_claim_until,
           started_at = v_now,
           available_at = v_now
     where r.run_id in (select c.run_id from candidate c)
    returning r.run_id, r.task_id, r.attempt
  ),
  task_upd as (
    update absurd.%2$I t
       set state = 'running',
           attempts = greatest(t.attempts, u.attempt),
           first_started_at = coalesce(t.first_started_at, v_now),
           last_attempt_run = u.run_id
      from updated u
     where t.task_id = u.task_id
    returning t.task_id
  ),
  wait_cleanup as (
    delete from absurd.%3$I w
     using updated u
    where w.run_id = u.run_id
      and w.timeout_at is not null
      and w.timeout_at <= v_now
    returning w.run_id
  )
  select
    u.run_id,
    u.task_id,
    u.attempt,
    t.task_name,
    t.params,
    t.retry_strategy,
    t.max_attempts,
    t.headers,
    r.wake_event,
    r.event_payload
  from updated u
  join absurd.%1$I r on r.run_id = u.run_id
  join absurd.%2$I t on t.task_id = u.task_id
  order by r.available_at, u.run_id;
end;
$body$,
    'r_' || p_queue_name,
    't_' || p_queue_name,
    'w_' || p_queue_name,
    p_queue_name
  );

  -- Nothing to do when the generated function is already in place.  Replacing
  -- it anyway would write a new pg_proc row on every create_queue(),
  -- invalidating every session's cached claim plans, and two transactions
  -- repeating create_queue() for the same queue would collide on that row
  -- ("tuple concurrently updated").
  if exists (
    select 1
      from pg_catalog.pg_proc p
     where p.pronamespace = 'absurd'::regnamespace
       and p.proname = v_fn_name
       and p.proargtypes = '25 23 23'::oidvector
       and p.proowner = v_table_owner
       and p.prosrc = v_body
  ) then
    return;
  end if;

  begin
    execute format(
      $tmpl$
create or replace function absurd.%1$I (
  p_worker_id text,
  p_claim_timeout integer default 30,
  p_qty integer default 1
)
  returns table (
    run_id uuid,
    task_id uuid,
    attempt integer,
    task_name text,
    params jsonb,
    retry_strategy jsonb,
    max_attempts integer,
    headers jsonb,
    wake_event text,
    event_payload jsonb
  )
  language plpgsql
as %2$L
      $tmpl$,
      v_fn_name,
      v_body
    );

    select p.proowner into v_fn_owner
      from pg_catalog.pg_proc p
     where p.proname = v_fn_name
       and p.pronamespace = 'absurd'::regnamespace
       and p.proargtypes = '25 23 23'::oidvector;

    if v_fn_owner <> v_table_owner then
      execute format(
        'alter function absurd.%I(text, integer, integer) owner to %I',
        v_fn_name,
        pg_catalog.pg_get_userbyid(v_table_owner)
      );
    end if;
  exception
    when insufficient_privilege then
      -- The caller may not create or replace the function, or may not hand it
      -- to the table owner.  The whole block is rolled back, so no function
      -- owned by the wrong role is left behind; claim_task() keeps using its
      -- dynamic statements for this queue.
      raise notice 'Not creating claim function absurd.%: %', v_fn_name, sqlerrm;
  end;
end;
$$;

create or replace function absurd.ensure_queue_tables (p_queue_name text)
  returns void
  language plpgsql
as $$
declare
  v_storage_mode text;
  v_t_suffix text;
  v_r_suffix text;
  v_c_suffix text;
  v_w_suffix text;
  v_t_idempotency_def text;
begin
  perform absurd.validate_queue_name(p_queue_name);

  -- Lock the queue's row before any table or index DDL, the order
  -- drop_queue() takes them in (row, then tables).  Taking table locks first
  -- (CREATE INDEX IF NOT EXISTS locks an existing table) and the row later, as
  -- ensure_claim_function() does, deadlocks with a concurrent drop_queue().
  -- A caller without UPDATE on absurd.queues cannot lock the row; it reads
  -- the row unlocked, is not serialized with drop_queue() (it never waits for
  -- the row, so the two cannot deadlock either), and gets no claim function
  -- (ensure_claim_function()).
  begin
    select q.storage_mode into v_storage_mode
      from absurd.queues q
     where q.queue_name = p_queue_name
       for no key update;
  exception
    when insufficient_privilege then
      select q.storage_mode into v_storage_mode
        from absurd.queues q
       where q.queue_name = p_queue_name;
  end;

  -- No row: the queue never existed, or a concurrent drop_queue() removed it
  -- while this waited for the row.
  if v_storage_mode is null then
    raise exception 'Queue "%" does not exist', p_queue_name;
  end if;

  if v_storage_mode not in ('unpartitioned', 'partitioned') then
    raise exception 'Unsupported queue storage mode "%"', v_storage_mode;
  end if;

  if v_storage_mode = 'partitioned' then
    v_t_suffix := 'partition by range (task_id)';
    v_r_suffix := 'partition by range (run_id)';
    v_c_suffix := 'partition by range (task_id)';
    v_w_suffix := 'partition by range (run_id)';
    v_t_idempotency_def := 'idempotency_key text';
  else
    v_t_suffix := 'with (fillfactor=70)';
    v_r_suffix := 'with (fillfactor=70)';
    v_c_suffix := 'with (fillfactor=70)';
    v_w_suffix := '';
    v_t_idempotency_def := 'idempotency_key text unique';
  end if;

  execute format(
    'create table if not exists absurd.%I (
        task_id uuid primary key,
        task_name text not null,
        params jsonb not null,
        headers jsonb,
        retry_strategy jsonb,
        max_attempts integer,
        cancellation jsonb,
        enqueue_at timestamptz not null default absurd.current_time(),
        first_started_at timestamptz,
        state text not null check (state in (''pending'', ''running'', ''sleeping'', ''completed'', ''failed'', ''cancelled'')),
        attempts integer not null default 0,
        last_attempt_run uuid,
        completed_payload jsonb,
        cancelled_at timestamptz,
        %s
     ) %s',
    't_' || p_queue_name,
    v_t_idempotency_def,
    v_t_suffix
  );

  execute format(
    'create table if not exists absurd.%I (
        run_id uuid primary key,
        task_id uuid not null,
        attempt integer not null,
        state text not null check (state in (''pending'', ''running'', ''sleeping'', ''completed'', ''failed'', ''cancelled'')),
        claimed_by text,
        claim_expires_at timestamptz,
        available_at timestamptz not null,
        wake_event text,
        event_payload jsonb,
        started_at timestamptz,
        completed_at timestamptz,
        failed_at timestamptz,
        result jsonb,
        failure_reason jsonb,
        created_at timestamptz not null default absurd.current_time()
     ) %s',
    'r_' || p_queue_name,
    v_r_suffix
  );

  execute format(
    'create table if not exists absurd.%I (
        task_id uuid not null,
        checkpoint_name text not null,
        state jsonb,
        status text not null default ''committed'',
        owner_run_id uuid,
        updated_at timestamptz not null default absurd.current_time(),
        primary key (task_id, checkpoint_name)
     ) %s',
    'c_' || p_queue_name,
    v_c_suffix
  );

  execute format(
    'create table if not exists absurd.%I (
        event_name text primary key,
        payload jsonb,
        emitted_at timestamptz not null default absurd.current_time()
     )',
    'e_' || p_queue_name
  );

  execute format(
    'create table if not exists absurd.%I (
        task_id uuid not null,
        run_id uuid not null,
        step_name text not null,
        event_name text not null,
        timeout_at timestamptz,
        created_at timestamptz not null default absurd.current_time(),
        primary key (run_id, step_name)
     ) %s',
    'w_' || p_queue_name,
    v_w_suffix
  );

  if v_storage_mode = 'partitioned' then
    execute format(
      'create table if not exists absurd.%I (
          idempotency_key text primary key,
          task_id uuid not null
       )',
      'i_' || p_queue_name
    );
  end if;

  execute format(
    'create index if not exists %I on absurd.%I (state, available_at)',
    ('r_' || p_queue_name) || '_sai',
    'r_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on absurd.%I (task_id)',
    ('r_' || p_queue_name) || '_ti',
    'r_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on absurd.%I (claim_expires_at)
      where state = ''running''
        and claim_expires_at is not null',
    ('r_' || p_queue_name) || '_cei',
    'r_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on absurd.%I (event_name)',
    ('w_' || p_queue_name) || '_eni',
    'w_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on absurd.%I (task_id)',
    ('w_' || p_queue_name) || '_ti',
    'w_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on absurd.%I (emitted_at)',
    ('e_' || p_queue_name) || '_eai',
    'e_' || p_queue_name
  );

  if v_storage_mode = 'partitioned' then
    execute format(
      'create index if not exists %I on absurd.%I (task_id)',
      ('i_' || p_queue_name) || '_ti',
      'i_' || p_queue_name
    );

    perform absurd.ensure_partitions(p_queue_name);
  end if;

  -- The claim function is optional (claim_task() falls back to dynamic SQL).
  -- A role that may run this function but not ensure_claim_function(), for
  -- example after an upgrade in an installation that withholds PUBLIC
  -- EXECUTE, still creates its queue.
  if pg_catalog.has_function_privilege(
       'absurd.ensure_claim_function(text)', 'execute'
     ) then
    perform absurd.ensure_claim_function(p_queue_name);
  end if;
end;
$$;

-- Drop a queue if it exists.
-- We intentionally don't validate the provided name here so legacy queues
-- created under older naming rules can still be removed.
create or replace function absurd.drop_queue (p_queue_name text)
  returns void
  language plpgsql
as $$
declare
  v_existing_queue text;
begin
  -- Delete the queue's row first: the row lock this takes makes a concurrent
  -- ensure_claim_function() for the queue wait, and then find no queue,
  -- instead of creating a claim function for tables dropped below.
  delete from absurd.queues
   where queue_name = p_queue_name
  returning queue_name into v_existing_queue;

  if v_existing_queue is null then
    return;
  end if;

  -- Remove queue-scoped maintenance jobs only when pg_cron is available.
  if to_regclass('cron.job') is not null and exists (
    select 1
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'cron'
      and p.proname = 'unschedule'
  ) then
    perform absurd.disable_cron(p_queue_name);
  end if;

  -- Per-queue claim function (see ensure_claim_function).  A function of that
  -- name owned by a role the caller cannot act as was not created by
  -- ensure_claim_function() (claim_task() ignores it too); it is left to its
  -- owner rather than preventing the queue owner from dropping the queue.
  begin
    execute format(
      'drop function if exists absurd.%I(text, integer, integer)',
      'ct_' || p_queue_name
    );
  exception
    when insufficient_privilege then
      raise notice 'Not dropping function absurd.%: %', 'ct_' || p_queue_name, sqlerrm;
  end;

  execute format('drop table if exists absurd.%I cascade', 'i_' || p_queue_name);
  execute format('drop table if exists absurd.%I cascade', 'w_' || p_queue_name);
  execute format('drop table if exists absurd.%I cascade', 'e_' || p_queue_name);
  execute format('drop table if exists absurd.%I cascade', 'c_' || p_queue_name);
  execute format('drop table if exists absurd.%I cascade', 'r_' || p_queue_name);
  execute format('drop table if exists absurd.%I cascade', 't_' || p_queue_name);
end;
$$;

-- Workers call this to reserve a task from a given queue
-- for a given reservation period in seconds.
--
-- The work is done by the per-queue function absurd.ct_<queue> when it
-- exists (cached plans); the dynamic statements below are the fallback and
-- must stay semantically identical to the generated ones.
create or replace function absurd.claim_task (
  p_queue_name text,
  p_worker_id text,
  p_claim_timeout integer default 30,
  p_qty integer default 1
)
  returns table (
    run_id uuid,
    task_id uuid,
    attempt integer,
    task_name text,
    params jsonb,
    retry_strategy jsonb,
    max_attempts integer,
    headers jsonb,
    wake_event text,
    event_payload jsonb
  )
  language plpgsql
as $$
declare
  v_now timestamptz;
  v_claim_timeout integer;
  v_worker_id text;
  v_qty integer;
  v_claim_until timestamptz := null;
  v_sql text;
  v_expired_run record;
  v_cancel_candidate record;
  v_expired_sweep_limit integer;
  v_fn_name text := 'ct_' || p_queue_name;
begin
  -- Prefer the per-queue function created by ensure_claim_function(): its
  -- statements are static SQL, so their plans are cached.  Queues without one
  -- (created before it existed and not yet migrated) use the dynamic
  -- statements below, and so does a caller that lacks EXECUTE on it: this
  -- function is SECURITY INVOKER, so the generated function does not inherit
  -- the caller's EXECUTE on claim_task().
  --
  -- Trust boundary.  The function runs with the CALLER's privileges, so
  -- whoever owns it can run arbitrary code as every role that claims from this
  -- queue.  The check below establishes that the function is callable
  -- (has_function_privilege) and that it is owned by the owner of the queue's
  -- tables, not that it is authentic.  The ownership test stops a role that
  -- merely holds CREATE on schema absurd from planting ct_<queue> for a queue
  -- that has none.  It does not stop the owner of the queue's tables, a
  -- superuser, or a member of the owning role from replacing the function, and
  -- it does not protect against anything those roles can already do to the
  -- tables themselves (for example, a trigger).  Schema creators and queue
  -- owners must be trusted by the roles that claim from those queues.
  --
  -- Everything the dynamic statements need is initialized below this check so
  -- the fast path does no other work.
  if exists (
    select 1
      from pg_catalog.pg_proc p
      join pg_catalog.pg_class c
        on c.relnamespace = p.pronamespace
       and c.relname = 't_' || p_queue_name
     where p.proname = v_fn_name
       and p.pronamespace = 'absurd'::regnamespace
       and p.proargtypes = '25 23 23'::oidvector
       and p.proowner = c.relowner
       and pg_catalog.has_function_privilege(p.oid, 'execute')
  ) then
    return query execute format('select * from absurd.%I($1, $2, $3)', v_fn_name)
      using p_worker_id, p_claim_timeout, p_qty;
    return;
  end if;

  v_now := absurd.current_time();
  v_claim_timeout := greatest(coalesce(p_claim_timeout, 30), 0);
  v_worker_id := coalesce(nullif(p_worker_id, ''), 'worker');
  v_qty := greatest(coalesce(p_qty, 1), 1);

  if v_claim_timeout > 0 then
    v_claim_until := v_now + make_interval(secs => v_claim_timeout);
  end if;

  -- Keep claim polling work bounded: process at most v_qty expired leases
  -- per claim call.
  v_expired_sweep_limit := greatest(v_qty, 1);

  -- Apply cancellation rules before claiming.
  --
  -- Use cancel_task() so lock order stays consistent (runs first, task second)
  -- with complete_run()/fail_run().
  for v_cancel_candidate in
    execute format(
      'select task_id
         from absurd.%I
        where state in (''pending'', ''sleeping'', ''running'')
          and (
            (
              (cancellation->>''max_delay'')::bigint is not null
              and first_started_at is null
              and extract(epoch from ($1 - enqueue_at)) >= (cancellation->>''max_delay'')::bigint
            )
            or
            (
              (cancellation->>''max_duration'')::bigint is not null
              and first_started_at is not null
              and extract(epoch from ($1 - first_started_at)) >= (cancellation->>''max_duration'')::bigint
            )
          )
        order by task_id',
      't_' || p_queue_name
    )
  using v_now
  loop
    perform absurd.cancel_task(p_queue_name, v_cancel_candidate.task_id);
  end loop;

  for v_expired_run in
    execute format(
      'select run_id,
              claimed_by,
              claim_expires_at,
              attempt
         from absurd.%I
        where state = ''running''
          and claim_expires_at is not null
          and claim_expires_at <= $1
        order by claim_expires_at, run_id
        limit $2
        for update skip locked',
      'r_' || p_queue_name
    )
  using v_now, v_expired_sweep_limit
  loop
    perform absurd.fail_run(
      p_queue_name,
      v_expired_run.run_id,
      jsonb_strip_nulls(jsonb_build_object(
        'name', '$ClaimTimeout',
        'message', 'worker did not finish task within claim interval',
        'workerId', v_expired_run.claimed_by,
        'claimExpiredAt', v_expired_run.claim_expires_at,
        'attempt', v_expired_run.attempt
      )),
      null
    );
  end loop;

  v_sql := format(
    'with candidate as (
        select r.run_id
          from absurd.%1$I r
          join absurd.%2$I t on t.task_id = r.task_id
         where r.state in (''pending'', ''sleeping'')
           and t.state in (''pending'', ''sleeping'', ''running'')
           and r.available_at <= $1
         order by r.available_at, r.run_id
         limit $2
         for update skip locked
     ),
     updated as (
        update absurd.%1$I r
           set state = ''running'',
               claimed_by = $3,
               claim_expires_at = $4,
               started_at = $1,
               available_at = $1
         where run_id in (select run_id from candidate)
         returning r.run_id, r.task_id, r.attempt
     ),
     task_upd as (
        update absurd.%2$I t
           set state = ''running'',
               attempts = greatest(t.attempts, u.attempt),
               first_started_at = coalesce(t.first_started_at, $1),
               last_attempt_run = u.run_id
          from updated u
         where t.task_id = u.task_id
         returning t.task_id
     ),
     wait_cleanup as (
        delete from absurd.%3$I w
         using updated u
        where w.run_id = u.run_id
          and w.timeout_at is not null
          and w.timeout_at <= $1
        returning w.run_id
     )
     select
       u.run_id,
       u.task_id,
       u.attempt,
       t.task_name,
       t.params,
       t.retry_strategy,
       t.max_attempts,
      t.headers,
      r.wake_event,
      r.event_payload
     from updated u
     join absurd.%1$I r on r.run_id = u.run_id
     join absurd.%2$I t on t.task_id = u.task_id
     order by r.available_at, u.run_id',
    'r_' || p_queue_name,
    't_' || p_queue_name,
    'w_' || p_queue_name
  );

  return query execute v_sql using v_now, v_qty, v_worker_id, v_claim_until;
end;
$$;

create or replace function absurd.set_task_checkpoint_state (
  p_queue_name text,
  p_task_id uuid,
  p_step_name text,
  p_state jsonb,
  p_owner_run uuid,
  p_extend_claim_by integer default null
)
  returns void
  language plpgsql
as $$
declare
  v_now timestamptz := absurd.current_time();
  v_run_task_id uuid;
  v_new_attempt integer;
  v_existing_attempt integer;
  v_existing_owner uuid;
  v_task_state text;
  v_run_state text;
begin
  if p_step_name is null or length(trim(p_step_name)) = 0 then
    raise exception 'step_name must be provided';
  end if;

  execute format(
    'select r.task_id, r.attempt, r.state, t.state
       from absurd.%I r
       join absurd.%I t on t.task_id = r.task_id
      where r.run_id = $1
      for update of r',
    'r_' || p_queue_name,
    't_' || p_queue_name
  )
  into v_run_task_id, v_new_attempt, v_run_state, v_task_state
  using p_owner_run;

  if v_run_task_id is null then
    raise exception 'Run "%" not found for checkpoint', p_owner_run;
  end if;

  if v_run_task_id <> p_task_id then
    raise exception 'Run "%" does not belong to task "%" in queue "%"', p_owner_run, p_task_id, p_queue_name;
  end if;

  if v_task_state = 'cancelled' then
    raise exception sqlstate 'AB001' using message = 'Task has been cancelled';
  end if;

  if v_run_state = 'failed' then
    raise exception sqlstate 'AB002' using message = format('Run "%s" has already failed in queue "%s"', p_owner_run, p_queue_name);
  end if;

  if v_task_state in ('completed', 'failed') or v_run_state in ('completed', 'cancelled') then
    raise exception 'Cannot checkpoint terminal run "%" or task "%" in queue "%"',
      p_owner_run,
      p_task_id,
      p_queue_name;
  end if;

  execute format(
    'select c.owner_run_id,
            r.attempt
       from absurd.%I c
       left join absurd.%I r on r.run_id = c.owner_run_id
      where c.task_id = $1
        and c.checkpoint_name = $2',
    'c_' || p_queue_name,
    'r_' || p_queue_name
  )
  into v_existing_owner, v_existing_attempt
  using p_task_id, p_step_name;

  if v_existing_owner is null or v_existing_attempt is null or v_new_attempt >= v_existing_attempt then
    execute format(
      'insert into absurd.%I (task_id, checkpoint_name, state, status, owner_run_id, updated_at)
       values ($1, $2, $3, ''committed'', $4, $5)
       on conflict (task_id, checkpoint_name)
       do update set state = excluded.state,
                     status = excluded.status,
                     owner_run_id = excluded.owner_run_id,
                     updated_at = excluded.updated_at',
      'c_' || p_queue_name
    ) using p_task_id, p_step_name, p_state, p_owner_run, v_now;
  end if;

  -- Extend the claim if requested, after the checkpoint write: that write can wait behind
  -- another transaction, and the renewal must count from when the claim is written.
  if p_extend_claim_by is not null and p_extend_claim_by > 0 then
    execute format(
      'update absurd.%I
          set claim_expires_at = $2 + make_interval(secs => $3)
        where run_id = $1
          and state = ''running''
          and claim_expires_at is not null',
      'r_' || p_queue_name
    )
    using p_owner_run, absurd.current_time(), p_extend_claim_by;
  end if;
end;
$$;

create or replace function absurd.extend_claim (
  p_queue_name text,
  p_run_id uuid,
  p_extend_by integer
)
  returns void
  language plpgsql
as $$
declare
  v_now timestamptz;
  v_task_state text;
  v_run_state text;
  v_claim_expires_at timestamptz;
begin
  if p_extend_by is null or p_extend_by <= 0 then
    raise exception 'extend_by must be > 0';
  end if;

  execute format(
    'select r.state,
            r.claim_expires_at,
            t.state
       from absurd.%I r
       join absurd.%I t on t.task_id = r.task_id
      where r.run_id = $1
      for update',
    'r_' || p_queue_name,
    't_' || p_queue_name
  )
  into v_run_state, v_claim_expires_at, v_task_state
  using p_run_id;

  if v_run_state is null then
    raise exception 'Run "%" not found in queue "%"', p_run_id, p_queue_name;
  end if;

  if v_task_state = 'cancelled' then
    raise exception sqlstate 'AB001' using message = 'Task has been cancelled';
  end if;

  if v_run_state <> 'running' then
    if v_run_state = 'failed' then
      raise exception sqlstate 'AB002' using message = format('Run "%s" has already failed in queue "%s"', p_run_id, p_queue_name);
    end if;
    raise exception 'Run "%" is not currently running in queue "%"', p_run_id, p_queue_name;
  end if;

  if v_claim_expires_at is null then
    raise exception 'Run "%" does not have an active claim in queue "%"', p_run_id, p_queue_name;
  end if;

  -- Renew from when the claim is written: the row lock above can wait behind another
  -- transaction, and a time read before that wait can renew to an expiry already past.
  v_now := absurd.current_time();

  execute format(
    'update absurd.%I
        set claim_expires_at = $2 + make_interval(secs => $3)
      where run_id = $1',
    'r_' || p_queue_name
  )
  using p_run_id, v_now, p_extend_by;
end;
$$;

create or replace function absurd.await_event (
  p_queue_name text,
  p_task_id uuid,
  p_run_id uuid,
  p_step_name text,
  p_event_name text,
  p_timeout integer default null
)
  returns table (
    should_suspend boolean,
    payload jsonb
  )
  language plpgsql
as $$
declare
  v_run_task_id uuid;
  v_run_state text;
  v_existing_payload jsonb;
  v_event_payload jsonb;
  v_checkpoint_payload jsonb;
  v_resolved_payload jsonb;
  v_timeout_at timestamptz;
  v_available_at timestamptz;
  v_now timestamptz := absurd.current_time();
  v_task_state text;
  v_wake_event text;
  v_event_locked boolean;
begin
  if p_event_name is null or length(trim(p_event_name)) = 0 then
    raise exception 'event_name must be provided';
  end if;

  if p_timeout is not null then
    if p_timeout < 0 then
      raise exception 'timeout must be non-negative';
    end if;
    v_timeout_at := v_now + (p_timeout::double precision * interval '1 second');
  end if;

  v_available_at := coalesce(v_timeout_at, 'infinity'::timestamptz);

  execute format(
    'select task_id
       from absurd.%I
      where run_id = $1',
    'r_' || p_queue_name
  )
  into v_run_task_id
  using p_run_id;

  if v_run_task_id is null then
    raise exception 'Run "%" not found while awaiting event', p_run_id;
  end if;

  if v_run_task_id <> p_task_id then
    raise exception 'Run "%" does not belong to task "%" in queue "%"', p_run_id, p_task_id, p_queue_name;
  end if;

  execute format(
    'select state
       from absurd.%I
      where task_id = $1
        and checkpoint_name = $2',
    'c_' || p_queue_name
  )
  into v_checkpoint_payload
  using p_task_id, p_step_name;

  if v_checkpoint_payload is not null then
    return query select false, v_checkpoint_payload;
    return;
  end if;

  -- Ensure a row exists for this event so we can take a row-level lock.
  --
  -- We use payload IS NULL as the sentinel for "not emitted yet".  emit_event
  -- always writes a non-NULL payload (at minimum JSON null).
  --
  -- Lock ordering is important to avoid deadlocks: await_event locks the event
  -- row first (FOR SHARE) and then the run row (FOR UPDATE).  emit_event
  -- naturally locks the event row via its UPSERT before touching waits/runs.
  --
  -- ON CONFLICT DO NOTHING keeps no lock on an existing row, and
  -- cleanup_events() can delete that row before it is locked here.  Without a
  -- locked row an emission could commit between the payload lookup and the
  -- wait registration below without seeing the waiter, so repeat until a row
  -- is actually locked.
  loop
    execute format(
      'insert into absurd.%I (event_name, payload, emitted_at)
       values ($1, null, ''epoch''::timestamptz)
       on conflict (event_name) do nothing',
      'e_' || p_queue_name
    ) using p_event_name;

    execute format(
      'select true
         from absurd.%I
        where event_name = $1
        for share',
      'e_' || p_queue_name
    )
    into v_event_locked
    using p_event_name;

    exit when v_event_locked;
  end loop;

  execute format(
    'select r.task_id, r.state, r.event_payload, r.wake_event, t.state
       from absurd.%I r
       join absurd.%I t on t.task_id = r.task_id
      where r.run_id = $1
      for update',
    'r_' || p_queue_name,
    't_' || p_queue_name
  )
  into v_run_task_id, v_run_state, v_existing_payload, v_wake_event, v_task_state
  using p_run_id;

  if v_run_state is null then
    raise exception 'Run "%" not found while awaiting event', p_run_id;
  end if;

  if v_run_task_id <> p_task_id then
    raise exception 'Run "%" does not belong to task "%" in queue "%"', p_run_id, p_task_id, p_queue_name;
  end if;

  if v_task_state = 'cancelled' then
    raise exception sqlstate 'AB001' using message = 'Task has been cancelled';
  end if;

  execute format(
    'select payload
       from absurd.%I
      where event_name = $1',
    'e_' || p_queue_name
  )
  into v_event_payload
  using p_event_name;

  if v_existing_payload is not null then
    execute format(
      'update absurd.%I
          set event_payload = null
        where run_id = $1',
      'r_' || p_queue_name
    ) using p_run_id;

    if v_event_payload is not null and v_event_payload = v_existing_payload then
      v_resolved_payload := v_existing_payload;
    end if;
  end if;

  if v_run_state <> 'running' then
    raise exception 'Run "%" must be running to await events', p_run_id;
  end if;

  -- A timeout wake-up wins over an event emitted after the wait expired.
  -- emit_event deliberately excludes expired waits, so consuming that later
  -- global event here would disagree with the wake-up decision already made.
  if v_wake_event = p_event_name and v_existing_payload is null then
    execute format(
      'update absurd.%I set wake_event = null where run_id = $1',
      'r_' || p_queue_name
    ) using p_run_id;
    return query select false, null::jsonb;
    return;
  end if;

  if v_resolved_payload is null and v_event_payload is not null then
    v_resolved_payload := v_event_payload;
  end if;

  if v_resolved_payload is not null then
    execute format(
      'insert into absurd.%I (task_id, checkpoint_name, state, status, owner_run_id, updated_at)
       values ($1, $2, $3, ''committed'', $4, $5)
       on conflict (task_id, checkpoint_name)
       do update set state = excluded.state,
                     status = excluded.status,
                     owner_run_id = excluded.owner_run_id,
                     updated_at = excluded.updated_at',
      'c_' || p_queue_name
    ) using p_task_id, p_step_name, v_resolved_payload, p_run_id, v_now;
    return query select false, v_resolved_payload;
    return;
  end if;

  execute format(
    'insert into absurd.%I (task_id, run_id, step_name, event_name, timeout_at, created_at)
     values ($1, $2, $3, $4, $5, $6)
     on conflict (run_id, step_name)
     do update set event_name = excluded.event_name,
                   timeout_at = excluded.timeout_at,
                   created_at = excluded.created_at',
    'w_' || p_queue_name
  ) using p_task_id, p_run_id, p_step_name, p_event_name, v_timeout_at, v_now;

  execute format(
    'update absurd.%I
        set state = ''sleeping'',
            claimed_by = null,
            claim_expires_at = null,
            available_at = $3,
            wake_event = $2,
            event_payload = null
      where run_id = $1',
    'r_' || p_queue_name
  ) using p_run_id, p_event_name, v_available_at;

  execute format(
    'update absurd.%I
        set state = ''sleeping''
      where task_id = $1',
    't_' || p_queue_name
  ) using p_task_id;

  return query select true, null::jsonb;
  return;
end;
$$;

-- Cleans up old completed, failed, or cancelled tasks and their related data.
-- Deletes up to p_limit tasks whose terminal timestamp (completed_at,
-- failed_at, or cancelled_at) is older than the specified TTL in seconds,
-- oldest first.
--
-- Locks are taken runs first, then tasks, the order complete_run(), fail_run(),
-- extend_claim() and cancel_task() use: taking the task first would deadlock
-- with an operation that holds one of its runs and waits for the task.  Tasks
-- with a run or the task row locked by someone else are skipped (left for a
-- later call), and eligibility is checked again once the task is locked.
--
-- Skipped tasks do not count against p_limit.  Candidates are read in pages
-- in (terminal timestamp, task_id) order, until p_limit tasks are deleted or
-- no eligible task is left, so a locked oldest task does not stall cleanup and
-- concurrent cleanups move on past each other's tasks.  The terminal timestamp
-- has no index, so every page scans the eligible backlog and keeps only its
-- page in a top-N sort.  The first page holds p_limit tasks, as one batch did
-- before; a page after one that skipped tasks is twice as large (and never
-- smaller than the deletions still allowed), so skipping K locked tasks costs
-- about log2(K) scans instead of one scan per skipped task, and no page sorts
-- more than about twice the tasks examined.  Locks taken for a task that is
-- then skipped or left over are released again (see step 2 and 3 below).
--
-- Returns the number of tasks deleted.
create or replace function absurd.cleanup_tasks (
  p_queue_name text,
  p_ttl_seconds integer,
  p_limit integer default 1000
)
  returns integer
  language plpgsql
as $$
declare
  v_now timestamptz := absurd.current_time();
  v_cutoff timestamptz;
  v_deleted_count integer := 0;
  v_page_deleted integer;
  v_storage_mode text := 'unpartitioned';
  v_task_ids uuid[];
  v_locked_run_ids uuid[];
  v_accepted_ids uuid[];
  v_after_terminal_at timestamptz;
  v_after_task_id uuid;
  v_page_size bigint := p_limit;
begin
  if p_ttl_seconds is null or p_ttl_seconds < 0 then
    raise exception 'TTL must be a non-negative number of seconds';
  end if;

  if p_limit is null or p_limit < 1 then
    raise exception 'cleanup limit must be at least 1';
  end if;

  v_cutoff := v_now - (p_ttl_seconds * interval '1 second');

  select storage_mode into v_storage_mode
  from absurd.queues
  where queue_name = p_queue_name;

  v_storage_mode := coalesce(v_storage_mode, 'unpartitioned');

  while v_deleted_count < p_limit loop
    if v_after_task_id is not null then
      v_page_size := greatest(v_page_size * 2, p_limit - v_deleted_count);
    end if;

    -- 1. The next page of eligible tasks after the last one looked at, without
    --    locking anything yet.  A task is eligible once it is terminal and its
    --    terminal timestamp (that of its latest run for completed and failed
    --    tasks) is older than the cutoff.
    execute format(
      'select coalesce(array_agg(e.task_id order by e.terminal_at, e.task_id), ''{}''),
              (array_agg(e.terminal_at order by e.terminal_at desc, e.task_id desc))[1],
              (array_agg(e.task_id order by e.terminal_at desc, e.task_id desc))[1]
         from (
           select t.task_id, x.terminal_at
             from absurd.%1$I t
             left join absurd.%2$I r on r.run_id = t.last_attempt_run
            cross join lateral (
              select case
                       when t.state = ''completed'' then r.completed_at
                       when t.state = ''failed'' then r.failed_at
                       when t.state = ''cancelled'' then t.cancelled_at
                       else null
                     end as terminal_at
            ) x
            where t.state in (''completed'', ''failed'', ''cancelled'')
              and x.terminal_at < $1
              and ($3::timestamptz is null
                   or (x.terminal_at, t.task_id) > ($3::timestamptz, $4::uuid))
            order by x.terminal_at, t.task_id
            limit $2
         ) e',
      't_' || p_queue_name,
      'r_' || p_queue_name
    )
    into v_task_ids, v_after_terminal_at, v_after_task_id
    using v_cutoff, v_page_size, v_after_terminal_at, v_after_task_id;

    exit when cardinality(v_task_ids) = 0;

    -- 2. and 3. Lock the candidates' runs, then the candidates, skipping
    --    what someone else holds.  A candidate is accepted when all its runs
    --    and the task itself are locked here and the task is still eligible,
    --    oldest first and no more than the deletions still allowed.  The
    --    locks are taken in a subtransaction, which is rolled back (releasing
    --    every lock it took) unless all candidates were accepted; the accepted
    --    ones are then locked again on their own.  So only tasks about to be
    --    deleted, and their runs, stay locked.
    loop
      exit when cardinality(v_task_ids) = 0;

      begin
        execute format(
          'select coalesce(array_agg(run_id), ''{}'')
             from (
               select r.run_id
                 from absurd.%1$I r
                where r.task_id = any($1)
                order by r.run_id
                  for update skip locked
             ) l',
          'r_' || p_queue_name
        )
        into v_locked_run_ids
        using v_task_ids;

        execute format(
          'select coalesce(array_agg(task_id order by n), ''{}'')
             from (
               select t.task_id, c.n
                 from unnest($1) with ordinality as c(task_id, n)
                 join absurd.%1$I t on t.task_id = c.task_id
                 left join absurd.%2$I r on r.run_id = t.last_attempt_run
                where t.state in (''completed'', ''failed'', ''cancelled'')
                  and case
                        when t.state = ''completed'' then r.completed_at
                        when t.state = ''failed'' then r.failed_at
                        when t.state = ''cancelled'' then t.cancelled_at
                        else null
                      end < $2
                  and not exists (
                    select 1
                      from absurd.%2$I o
                     where o.task_id = c.task_id
                       and o.run_id <> all($4)
                  )
                order by c.n
                limit $3
                  for update of t skip locked
             ) l',
          't_' || p_queue_name,
          'r_' || p_queue_name
        )
        into v_accepted_ids
        using v_task_ids, v_cutoff, p_limit - v_deleted_count, v_locked_run_ids;

        if v_accepted_ids <> v_task_ids then
          raise exception using errcode = 'AB001';
        end if;
      exception
        when sqlstate 'AB001' then
          -- Rolled back; try again with the accepted candidates only.
          v_task_ids := v_accepted_ids;
          continue;
      end;

      exit;
    end loop;

    continue when cardinality(v_task_ids) = 0;

    -- 4. Delete in order: wait registrations, checkpoints, runs, idempotency
    --    keys (partitioned queues keep them in a side table; unpartitioned
    --    queues keep them on the task row), then tasks.
    execute format(
      'delete from absurd.%I w where w.task_id = any($1)',
      'w_' || p_queue_name
    ) using v_task_ids;

    execute format(
      'delete from absurd.%I c where c.task_id = any($1)',
      'c_' || p_queue_name
    ) using v_task_ids;

    execute format(
      'delete from absurd.%I r where r.task_id = any($1)',
      'r_' || p_queue_name
    ) using v_task_ids;

    if v_storage_mode = 'partitioned' then
      execute format(
        'delete from absurd.%I i where i.task_id = any($1)',
        'i_' || p_queue_name
      ) using v_task_ids;
    end if;

    execute format(
      'delete from absurd.%I t where t.task_id = any($1)',
      't_' || p_queue_name
    ) using v_task_ids;

    get diagnostics v_page_deleted = row_count;
    v_deleted_count := v_deleted_count + v_page_deleted;
  end loop;

  return v_deleted_count;
end;
$$;

-- Cleans up old emitted events.
-- Deletes events whose emitted_at timestamp is older than the specified TTL in seconds.
--
-- Returns the number of events deleted.
create or replace function absurd.cleanup_events (
  p_queue_name text,
  p_ttl_seconds integer,
  p_limit integer default 1000
)
  returns integer
  language plpgsql
as $$
declare
  v_now timestamptz := absurd.current_time();
  v_cutoff timestamptz;
  v_deleted_count integer;
begin
  if p_ttl_seconds is null or p_ttl_seconds < 0 then
    raise exception 'TTL must be a non-negative number of seconds';
  end if;

  if p_limit is null or p_limit < 1 then
    raise exception 'cleanup limit must be at least 1';
  end if;

  v_cutoff := v_now - (p_ttl_seconds * interval '1 second');

  execute format(
    'with to_delete as (
        select e.event_name
          from absurd.%I e
         where e.emitted_at < $1
         order by e.emitted_at
         limit $2
         for update of e skip locked
     ),
     del_events as (
        delete from absurd.%I e
         where e.event_name in (select event_name from to_delete)
         returning 1
     )
     select count(*) from del_events',
    'e_' || p_queue_name,
    'e_' || p_queue_name
  )
  into v_deleted_count
  using v_cutoff, p_limit;

  return v_deleted_count;
end;
$$;

-- Make weekly partition tags year-unique and rename existing weekly
-- partitions to them, in one statement so that the tag function and the
-- partition names change together even when this file is not applied as one
-- transaction.
--
-- 0.5.0 named weekly partitions with a three-digit tag (last digit of the ISO
-- year plus the week).  partition_week_tag() now returns IYYYIW, so
-- ensure_partitions() would try to create a second, overlapping partition for
-- every week that already has one.  The new tag is derived from each
-- partition's lower bound (a UUIDv7 floor, whose first 48 bits are the
-- millisecond timestamp).  Default partitions (_d) are untouched, and so are
-- partitions that already carry a new-style tag.  Safe to re-run.
do $$
declare
  v_partition record;
  v_lower uuid;
  v_new_name text;
  v_index record;
begin
  -- Returns a weekly partition tag in IYYYIW format, where:
  -- * IYYY = complete ISO year in UTC
  -- * WW = zero-padded ISO week number in UTC (01..53)
  --
  -- ISO weeks do not have week 0; days at year boundaries can belong
  -- to week 52/53 of the previous ISO year.
  --
  -- Examples:
  -- * 2024-01-01 UTC -> 202401
  -- * 2021-01-01 UTC -> 202053 (ISO week 53 of ISO year 2020)
  --
  -- The definition is that of sql/absurd.sql, so it is not indented here.
create or replace function absurd.partition_week_tag (p_ts timestamptz)
  returns text
  language sql
  immutable
  strict
as $fn$
  with bucket as (
    select absurd.week_bucket_utc(p_ts) at time zone 'UTC' as ts
  )
  select
    (extract(isoyear from ts)::int)::text ||
    lpad((extract(week from ts)::int)::text, 2, '0')
  from bucket;
$fn$;

  -- Take ACCESS EXCLUSIVE on the queue registry before listing anything.
  -- Every function that creates partitions or queues reads absurd.queues
  -- first (ensure_partitions(), create_queue(), set_queue_policy()), and
  -- drop_queue() deletes from it, so this waits for each such call in
  -- progress, including one that has computed an old-style tag and waits for
  -- a partition lock, and makes every later call wait until this transaction
  -- commits and then use the new partition_week_tag().  No partition or queue
  -- therefore appears after the list below is read.  It is one lock, so it
  -- cannot be taken in an order that deadlocks with the registry-row-first
  -- order of those functions, and when this file is applied as one
  -- transaction it is held through the claim function backfill, whose row
  -- locks then never wait for another session.  It also blocks plain reads
  -- of absurd.queues (spawn_task() and the maintenance functions) until this
  -- transaction ends; a long transaction that has read absurd.queues delays
  -- it (with lock_timeout set the statement fails, changes nothing, and the
  -- file can be applied again).
  --
  -- A queue created concurrently that owns a new partition name makes the
  -- rename below fail with "already exists" and change nothing; the
  -- collision preflight above lists it on the next run.  One over the name
  -- limit is refused right here.
  lock table absurd.queues in access exclusive mode;

  -- A queue over the name limit created after the first preflight: its new
  -- partition names would be truncated.  Refuse as that preflight does.
  if exists (select 1 from absurd.queues where octet_length(queue_name) > 53) then
    raise exception 'Cannot migrate to main: a queue name over 53 bytes was created during the migration'
      using hint = 'Apply this migration again to list the queues and the remediation.',
            errcode = 'check_violation';
  end if;

  for v_partition in
    select child.oid as child_oid,
           child.relname as child_name,
           parent.relname as parent_name,
           pg_catalog.pg_get_expr(child.relpartbound, child.oid) as bound
      from absurd.queues q
      join pg_catalog.pg_class parent
        on parent.relnamespace = 'absurd'::regnamespace
       and parent.relname in ('t_' || q.queue_name, 'r_' || q.queue_name,
                              'c_' || q.queue_name, 'w_' || q.queue_name)
      join pg_catalog.pg_inherits i on i.inhparent = parent.oid
      join pg_catalog.pg_class child on child.oid = i.inhrelid
     where q.storage_mode = 'partitioned'
       and substr(child.relname, length(parent.relname) + 1) ~ '^_[0-9]{3}$'
     order by child.relname
  loop
    v_lower := substring(v_partition.bound from 'FROM \(''([0-9a-f-]{36})''')::uuid;
    if v_lower is null then
      continue;
    end if;

    v_new_name := v_partition.parent_name || '_' || absurd.partition_week_tag(
      to_timestamp(
        ('x' || substr(replace(v_lower::text, '-', ''), 1, 12))::bit(48)::bigint
        / 1000.0
      )
    );

    execute format('alter table absurd.%I rename to %I', v_partition.child_name, v_new_name);

    -- Keep the generated index and constraint names in step with the table
    -- where the longer name still fits into an identifier.  PostgreSQL named
    -- the others itself (shortening them to fit), and they stay as they are:
    -- renaming them here would silently truncate them.
    for v_index in
      select ic.relname
        from pg_catalog.pg_index ix
        join pg_catalog.pg_class ic on ic.oid = ix.indexrelid
       where ix.indrelid = v_partition.child_oid
         and left(ic.relname, length(v_partition.child_name) + 1) = v_partition.child_name || '_'
         and octet_length(v_new_name)
             + octet_length(ic.relname) - octet_length(v_partition.child_name) <= 63
    loop
      execute format(
        'alter index absurd.%I rename to %I',
        v_index.relname,
        v_new_name || substr(v_index.relname, length(v_partition.child_name) + 1)
      );
    end loop;
  end loop;
end;
$$;


-- Create the claim function for every existing queue.  Safe to re-run.
-- Each function is owned by the owner of its queue's tables, not by the role
-- applying this migration.
do $$
declare
  v_queue record;
begin
  for v_queue in select queue_name from absurd.queues order by queue_name loop
    perform absurd.ensure_claim_function(v_queue.queue_name);
  end loop;
end;
$$;

-- Publish the new schema version last: absurdctl migrate applies this file
-- statement by statement and decides from this version whether it still has
-- to be applied, so a failure above leaves the database reporting 0.5.0 and
-- the next migrate applies the whole (re-runnable) file again.
create or replace function absurd.get_schema_version ()
  returns text
  language sql
as $$
  select 'main'::text;
$$;
