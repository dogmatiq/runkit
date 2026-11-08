--------------------------------------------------------------------------------
-- The "signal_create" trigger sends an "eventstream.create" signal through the
-- signal bus in internal/signal when an event stream is created. The local
-- event stream source in internal/eventstream/source watches for it to discover
-- new streams.
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

--------------------------------------------------------------------------------
-- The "signal_append" trigger sends an "eventstream.append" signal through the
-- signal bus in internal/signal when events are appended to an event stream.
-- The signal's subtopic is the stream ID. The local event stream source in
-- internal/eventstream/source watches each stream's signal to deliver its new
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
