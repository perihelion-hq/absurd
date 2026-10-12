-- Migration from 0.5.0 to main
--
-- Adds per-queue claim functions (absurd.ct_<queue>) so claim_task() runs
-- statically planned, plan-cached SQL instead of re-planning dynamic SQL on
-- every poll.  claim_task() keeps its signature and result columns and falls
-- back to its previous dynamic statements for any queue without a generated
-- function.  No tables or data are touched.
--
-- Note: this migration contains only the claim-function change.  Other
-- unreleased changes in sql/absurd.sql since 0.5.0 must still be incorporated
-- into this file at release time.

create or replace function absurd.get_schema_version ()
  returns text
  language sql
as $$
  select 'main'::text;
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
-- so it cannot be pointed at another queue.  Names that would not fit into a
-- 63-byte identifier (possible only for queues created under older, longer
-- naming limits) get no function and keep using claim_task()'s dynamic path.
create or replace function absurd.ensure_claim_function (p_queue_name text)
  returns void
  language plpgsql
as $$
declare
  v_fn_name text := 'ct_' || p_queue_name;
begin
  if octet_length(v_fn_name) > 63 then
    return;
  end if;

  execute format(
    $tmpl$
create or replace function absurd.%4$I (
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
as $fn$
#variable_conflict use_column
declare
  v_queue_name constant text := %5$L;
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
$fn$;
    $tmpl$,
    'r_' || p_queue_name,
    't_' || p_queue_name,
    'w_' || p_queue_name,
    v_fn_name,
    p_queue_name
  );
end;
$$;

create or replace function absurd.ensure_queue_tables (p_queue_name text)
  returns void
  language plpgsql
as $$
declare
  v_storage_mode text := 'unpartitioned';
  v_t_suffix text;
  v_r_suffix text;
  v_c_suffix text;
  v_w_suffix text;
  v_t_idempotency_def text;
begin
  perform absurd.validate_queue_name(p_queue_name);

  select storage_mode into v_storage_mode
  from absurd.queues
  where queue_name = p_queue_name;

  v_storage_mode := coalesce(v_storage_mode, 'unpartitioned');

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

  perform absurd.ensure_claim_function(p_queue_name);
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
  select queue_name into v_existing_queue
  from absurd.queues
  where queue_name = p_queue_name;

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

  -- Per-queue claim function (see ensure_claim_function).  Skipped when the
  -- name would be truncated so we never drop another queue's function.
  if octet_length('ct_' || p_queue_name) <= 63 then
    execute format(
      'drop function if exists absurd.%I(text, integer, integer)',
      'ct_' || p_queue_name
    );
  end if;

  execute format('drop table if exists absurd.%I cascade', 'i_' || p_queue_name);
  execute format('drop table if exists absurd.%I cascade', 'w_' || p_queue_name);
  execute format('drop table if exists absurd.%I cascade', 'e_' || p_queue_name);
  execute format('drop table if exists absurd.%I cascade', 'c_' || p_queue_name);
  execute format('drop table if exists absurd.%I cascade', 'r_' || p_queue_name);
  execute format('drop table if exists absurd.%I cascade', 't_' || p_queue_name);

  delete from absurd.queues where queue_name = p_queue_name;
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
  v_now timestamptz := absurd.current_time();
  v_claim_timeout integer := greatest(coalesce(p_claim_timeout, 30), 0);
  v_worker_id text := coalesce(nullif(p_worker_id, ''), 'worker');
  v_qty integer := greatest(coalesce(p_qty, 1), 1);
  v_claim_until timestamptz := null;
  v_sql text;
  v_expired_run record;
  v_cancel_candidate record;
  v_expired_sweep_limit integer;
  v_fn_name text := 'ct_' || p_queue_name;
begin
  -- Prefer the per-queue function created by ensure_claim_function(): its
  -- statements are static SQL, so their plans are cached.  Queues without one
  -- (created before it existed and not yet migrated, or with an over-long
  -- legacy name) use the dynamic statements below.
  if exists (
    select 1
      from pg_catalog.pg_proc p
     where p.proname = v_fn_name
       and p.pronamespace = 'absurd'::regnamespace
  ) then
    return query execute format('select * from absurd.%I($1, $2, $3)', v_fn_name)
      using p_worker_id, p_claim_timeout, p_qty;
    return;
  end if;

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

-- Create the claim function for every existing queue.  Safe to re-run.
do $$
declare
  v_queue record;
begin
  for v_queue in select queue_name from absurd.queues order by queue_name loop
    perform absurd.ensure_claim_function(v_queue.queue_name);
  end loop;
end;
$$;
