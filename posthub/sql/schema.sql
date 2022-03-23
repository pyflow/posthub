

CREATE TYPE posthub_message_state AS ENUM ('created','retry','active','completed','expired','cancelled', 'failed');

CREATE TABLE if not exists public.posthub_queue_config (
    que_name text DEFAULT 'default'::text PRIMARY KEY,
    fifo boolean DEFAULT 0 NOT NULL,
    msg_expires interval not null default interval '10 minutes',
    max_retries integer default -1;
    created timestamptz not null default now(),
    updated timestamptz not null default now()

)
WITH (fillfactor='90');

CREATE TABLE if not exists public.posthub_queue (
    id bigserial PRIMARY KEY,
    que_name text DEFAULT 'default'::text NOT NULL,
    state public.posthub_message_state not null default('created')
    retry_count integer DEFAULT 0 NOT NULL,
    completed_at timestamptz,
    expired_at timestamptz,
    cancelled_at timestamptz,
    enqueued_at timestamptz  NOT NULL DEFAULT current_timestamp,
    dequeued_at timestamptz,
    expected_at timestamptz,
    data jsonb DEFAULT '{}'::jsonb NOT NULL,
    msg_schema_version integer DEFAULT 1,
    CONSTRAINT queue_length CHECK ((char_length(que_name) <= 100)),
    CONSTRAINT valid_data CHECK ((jsonb_typeof(data) = 'object'::text))
)
WITH (fillfactor='90');

CREATE INDEX posthub_que_poll_idx
    ON posthub_queue (que_name, enqueued_at, id)
    WHERE (completed_at IS NULL AND expired_at IS NULL);

CREATE UNLOGGED TABLE if not exists public.posthub_queue_listener (
    pid integer NOT NULL,
    queues text[] NOT NULL,
    listening boolean NOT NULL,
    msg_schema_version integer DEFAULT 1,
    created timestamptz not null default now(),
    updated timestamptz not null default now()
    CONSTRAINT valid_queues CHECK (((array_ndims(queues) = 1) AND (array_length(queues, 1) IS NOT NULL)))
);
WITH (fillfactor='90');


CREATE FUNCTION que_determine_job_state(job public.posthub_queue) RETURNS text AS $$
  SELECT
    CASE
    WHEN job.expired_at  IS NOT NULL    THEN 'expired'
    WHEN job.completed_at IS NOT NULL    THEN 'completed'
    WHEN job.cancelled_at IS NOT NULL    THEN 'cancelled'
    WHEN job.retry_count = 0 AND job.expected_at > CURRENT_TIMESTAMP            THEN 'active'
    WHEN job.retry_count = 0 AND job.expected_at <= CURRENT_TIMESTAMP            THEN 'retry'
    WHEN job.retry_count > 0 AND job.expected_at > CURRENT_TIMESTAMP  THEN 'retry'
    ELSE                                     'created'
    END
$$
LANGUAGE SQL;

CREATE FUNCTION que_state_notify() RETURNS trigger AS $$
  DECLARE
    row record;
    message json;
    previous_state text;
    current_state text;
  BEGIN
    IF TG_OP = 'INSERT' THEN
      previous_state := 'nonexistent';
      current_state  := public.que_determine_job_state(NEW);
      row            := NEW;
    ELSIF TG_OP = 'DELETE' THEN
      previous_state := public.que_determine_job_state(OLD);
      current_state  := 'nonexistent';
      row            := OLD;
    ELSIF TG_OP = 'UPDATE' THEN
      previous_state := public.que_determine_job_state(OLD);
      current_state  := public.que_determine_job_state(NEW);

      -- If the state didn't change, short-circuit.
      IF previous_state = current_state THEN
        RETURN null;
      END IF;

      row := NEW;
    ELSE
      RAISE EXCEPTION 'Unrecognized TG_OP: %', TG_OP;
    END IF;

    SELECT row_to_json(t)
    INTO message
    FROM (
      SELECT
        'state_change' AS message_type,
        row.id       AS id,
        row.que_name    AS que_name,

        to_char(row.enqueued_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS enqueued_at,
        to_char(now()      AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS time,

        previous_state AS previous_state,
        current_state  AS current_state
    ) t;

    PERFORM pg_notify('que_state', message::text);

    RETURN null;
  END
$$
LANGUAGE plpgsql;

CREATE TRIGGER que_state_notify
  AFTER INSERT OR UPDATE OR DELETE ON que_jobs
  FOR EACH ROW
  EXECUTE PROCEDURE public.que_state_notify();


CREATE FUNCTION que_job_notify() RETURNS trigger AS $$
  DECLARE
    locker_pid integer;
    sort_key json;
  BEGIN
    IF locker_pid IS NOT NULL THEN
      -- There's a size limit to what can be broadcast via LISTEN/NOTIFY, so
      -- rather than throw errors when someone enqueues a big job, just
      -- broadcast the most pertinent information, and let the locker query for
      -- the record after it's taken the lock. The worker will have to hit the
      -- DB in order to make sure the job is still visible anyway.
      SELECT row_to_json(t)
      INTO sort_key
      FROM (
        SELECT
          'msg_available' AS message_type,
          NEW.que_name       AS que_name,
          NEW.id          AS message_id,
          -- Make sure we output timestamps as UTC ISO 8601
          to_char(NEW.enqueued_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS enqueued_at
      ) t;

      PERFORM pg_notify('que_listener_' || locker_pid::text, sort_key::text);
    END IF;

    RETURN null;
  END
$$
LANGUAGE plpgsql;

CREATE TRIGGER que_job_notify
  AFTER INSERT ON que_jobs
  FOR EACH ROW
  EXECUTE PROCEDURE public.que_job_notify();
