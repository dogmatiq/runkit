--------------------------------------------------------------------------------
-- Signal subscribers when event streams are created and when events are
-- appended to them.
--
-- The signals are sent by triggers rather than by the functions that write to
-- these tables so that every write path sends them. PostgreSQL delivers
-- identical notifications sent within one transaction only once, so a
-- transaction that appends many events to a stream produces a single signal.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION eventstream.signal_create()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('signal', 'eventstream.create.' || id) FROM inserted;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE TRIGGER signal_create
AFTER INSERT ON eventstream.streams
REFERENCING NEW TABLE AS inserted
FOR EACH STATEMENT
EXECUTE FUNCTION eventstream.signal_create();

CREATE OR REPLACE FUNCTION eventstream.signal_append()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('signal', 'eventstream.append.' || stream_id) FROM inserted;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE TRIGGER signal_append
AFTER INSERT ON eventstream.events
REFERENCING NEW TABLE AS inserted
FOR EACH STATEMENT
EXECUTE FUNCTION eventstream.signal_append();
