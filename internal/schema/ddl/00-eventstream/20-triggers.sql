--------------------------------------------------------------------------------
-- The "signal_create" trigger sends the "eventstream.create" signal when event
-- streams are created. source.Local in internal/eventstream/source waits for it
-- to discover new streams.
--
-- PostgreSQL delivers identical notifications sent within one transaction only
-- once, so a transaction that creates many streams produces a single signal.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION eventstream.signal_create()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('signal', 'eventstream.create') FROM inserted;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE TRIGGER signal_create
AFTER INSERT ON eventstream.streams
REFERENCING NEW TABLE AS inserted
FOR EACH STATEMENT
EXECUTE FUNCTION eventstream.signal_create();

--------------------------------------------------------------------------------
-- The "signal_append" trigger sends the "eventstream.append.<stream-id>" signal
-- when events are appended to an event stream. source.Local in
-- internal/eventstream/source waits for each stream's signal to deliver its new
-- events.
--
-- PostgreSQL delivers identical notifications sent within one transaction only
-- once, so a transaction that appends many events to a stream produces a single
-- signal.
--------------------------------------------------------------------------------
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
