

CREATE TYPE posthub_message_state AS ENUM ('created','retry','active', 'expired', 'failed');

CREATE TYPE posthub_message AS (
    id bigint,
    que_name text,
    state posthub_message_state,
    created timestamptz,
    fetched timestamptz,
    expired timestamptz,
    data jsonb,
    msg_schema_version integer
);

CREATE TABLE if not exists public.posthub_queue_config (
    que_name text DEFAULT 'default'::text PRIMARY KEY,
    retain_days integer default 4,
    msg_expires interval not null default interval '10 minutes',
    max_retries integer not null default 1000000,
    created timestamptz not null default now(),
    updated timestamptz not null default now()

)
WITH (fillfactor='90');

CREATE TABLE if not exists public.posthub_queue (
    id bigserial PRIMARY KEY,
    que_name text DEFAULT 'default'::text NOT NULL,
    state public.posthub_message_state not null default('created'),
    retry_count integer DEFAULT 0 NOT NULL,
    enqueued_at timestamptz  NOT NULL DEFAULT now(),
    scheduled_at timestamptz NOT NULL DEFAULT now(),
    dequeued_at timestamptz,
    expired_at timestamptz,
    data jsonb DEFAULT '{}'::jsonb NOT NULL,
    msg_schema_version integer DEFAULT 1,
    CONSTRAINT queue_length CHECK ((char_length(que_name) <= 200)),
    CONSTRAINT valid_data CHECK ((jsonb_typeof(data) = 'object'::text))
)
WITH (fillfactor='90');

CREATE INDEX posthub_queue_poll_idx
    ON posthub_queue (que_name, scheduled_at, msg_schema_version, id)
    WHERE (state in ('created', 'expired'));

CREATE INDEX posthub_queue_poll_working_idx
    ON posthub_queue (que_name, enqueued_at, id)
    WHERE (state in ('created', 'expired', 'active', 'retry'));

CREATE INDEX posthub_queue_poll_failed_idx
    ON posthub_queue (que_name, enqueued_at, id)
    WHERE (state in ('failed'));

CREATE INDEX posthub_queue_all_poll_active_idx
    ON posthub_queue (expired_at, id)
    WHERE (state in ('active', 'retry'));

CREATE INDEX posthub_queue_all_poll_enqueued_idx
    ON posthub_queue (enqueued_at, id);


CREATE FUNCTION posthub_que_determine_job_state(job public.posthub_queue, max_retry integer) RETURNS text AS $$
  SELECT
    CASE
    WHEN job.retry_count = 0 AND job.expired_at IS NULL THEN 'created'
    WHEN job.retry_count = 0 AND job.expired_at > CURRENT_TIMESTAMP            THEN 'active'
    WHEN job.retry_count = 0 AND job.expired_atf <= CURRENT_TIMESTAMP            THEN 'expired'
    WHEN job.retry_count > 0 AND job.retry_count < max_retry AND job.expired_at > CURRENT_TIMESTAMP  THEN 'retry'
    WHEN job.retry_count > 0 AND job.retry_count < max_retry AND job.expired_at <= CURRENT_TIMESTAMP  THEN 'expired'
    WHEN job.retry_count > 0 AND job.retry_count >= max_retry THEN 'failed'
    ELSE                                     'created'
    END;
$$
LANGUAGE SQL;

CREATE FUNCTION posthub_que_state_notify() RETURNS trigger AS $$
  DECLARE
    row record;
    message json;
    previous_state text;
    current_state text;
  BEGIN
    IF TG_OP = 'INSERT' THEN
      previous_state := 'nonexistent';
      current_state  := public.posthub_que_determine_job_state(NEW);
      row            := NEW;
    ELSIF TG_OP = 'DELETE' THEN
      previous_state := public.posthub_que_determine_job_state(OLD);
      current_state  := 'nonexistent';
      row            := OLD;
    ELSIF TG_OP = 'UPDATE' THEN
      previous_state := public.posthub_que_determine_job_state(OLD);
      current_state  := public.posthub_que_determine_job_state(NEW);

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

    PERFORM pg_notify('posthub_que_state', message::text);

    RETURN null;
  END
$$
LANGUAGE plpgsql;

CREATE TRIGGER posthub_que_state_notify
  AFTER INSERT OR UPDATE OR DELETE ON posthub_queue
  FOR EACH ROW
  EXECUTE PROCEDURE public.posthub_que_state_notify();


CREATE FUNCTION posthub_que_msg_notify() RETURNS trigger AS $$
  DECLARE
    sort_key json;
  BEGIN
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

    PERFORM pg_notify('posthub_queue_' || NEW.que_name::text, sort_key::text);

    RETURN null;
  END
$$
LANGUAGE plpgsql;

CREATE TRIGGER posthub_que_msg_notify
  AFTER INSERT ON posthub_queue
  FOR EACH ROW
  EXECUTE PROCEDURE public.posthub_que_msg_notify();

CREATE FUNCTION que_try_get_nowait(que_name text) RETURNS posthub_message AS $$
  DECLARE
    found_msg posthub_message;
    que_config posthub_queue_config%ROWTYPE;
  BEGIN
    SELECT config.*
            INTO STRICT que_config
            FROM posthub_queue_config as config
            WHERE
                config.que_name = $1;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'Queue % not found', $1;
            RETURN NULL;
        WHEN TOO_MANY_ROWS THEN
            RAISE NOTICE 'Queue % not unique', $1;
            RETURN NULL;

    FOR que_msg IN (SELECT * FROM posthub_queue
        WHERE que_name = $1 AND state in ('created', 'expired')
        ORDER BY scheduled_at
        LIMIT 10;) LOOP

        IF pg_try_advisory_lock(que_msg.id) THEN
            RETURN ROW(
                que_msg.id,
                que_msg.que_name,
                que_msg.state,
                que_msg.enqueued_at,
                now(),
                que_msg.expired_at,
                que_msg.data,
                que_msg.msg_schema_version
            );
        END IF;

    END LOOP;

    RETURN found_msg;
  END
$$
LANGUAGE plpgsql;

CREATE FUNCTION que_try_get_message_nowait(que_name text, id bigint) RETURNS posthub_message AS $$
  DECLARE
    que_config posthub_queue_config%ROWTYPE;
    que_msg  posthub_message%ROWTYPE;
  BEGIN
    SELECT config.*
        INTO STRICT que_config
        FROM posthub_queue_config as config
        WHERE
            config.que_name = $1;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'Queue % not found', $1;
            RETURN NULL;
        WHEN TOO_MANY_ROWS THEN
            RAISE NOTICE 'Queue % not unique', $1;
            RETURN NULL;

    IF NOT pg_try_advisory_lock($2) THEN
        RETURN NULL;
    END IF;

    SELECT * INTO STRICT que_msg FROM posthub_queue WHERE id = $2;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN NULL;

    RETURN ROW(
        que_msg.id,
        que_msg.que_name,
        que_msg.state,
        que_msg.enqueued_at,
        now(),
        que_msg.expired_at,
        que_msg.data,
        que_msg.msg_schema_version
    );
  END
$$
LANGUAGE plpgsql;

CREATE FUNCTION que_length(que_name text) RETURNS integer AS $$
  BEGIN
    RETURN (SELECT count(*) FROM posthub_queue
    WHERE que_name = $1 AND state in ('created', 'active', 'expired', 'retry'));
  END
$$
LANGUAGE plpgsql;


CREATE FUNCTION que_ack_message(que_name text, id bigint) RETURNS void AS $$
  DECLARE
    que_msg  posthub_message%ROWTYPE;
  BEGIN
    IF NOT pg_try_advisory_lock($2) THEN
        RETURN;
    END IF;

    DELETE * from posthub_queue WHERE id = $2 AND que_name = $1;
  END
$$
LANGUAGE plpgsql;


CREATE FUNCTION que_delete_message(que_name text, id bigint) RETURNS void AS $$
  DECLARE
    que_msg  posthub_message%ROWTYPE;
  BEGIN
    SELECT * INTO STRICT que_msg FROM posthub_queue WHERE id = $2;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN;

    DELETE * from posthub_queue WHERE id = $2 AND que_name = $1;
  END
$$
LANGUAGE plpgsql;


CREATE FUNCTION que_try_get_failed_nowait(que_name text) RETURNS posthub_message AS $$
  DECLARE
    found_msg posthub_message;
    que_config posthub_queue_config%ROWTYPE;
  BEGIN
    SELECT config.*
            INTO STRICT que_config
            FROM posthub_queue_config as config
            WHERE
                config.que_name = $1;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'Queue % not found', $1;
            RETURN NULL;
        WHEN TOO_MANY_ROWS THEN
            RAISE NOTICE 'Queue % not unique', $1;
            RETURN NULL;

    FOR que_msg IN (SELECT * FROM posthub_queue
        WHERE que_name = $1 AND state = 'failed'
        ORDER BY enqueued_at
        LIMIT 10;) LOOP

        IF pg_try_advisory_lock(que_msg.id) THEN
            RETURN ROW(
                que_msg.id,
                que_msg.que_name,
                que_msg.state,
                que_msg.enqueued_at,
                now(),
                que_msg.expired_at,
                que_msg.data,
                que_msg.msg_schema_version
            );
        END IF;

    END LOOP;

    RETURN found_msg;
  END
$$
LANGUAGE plpgsql;