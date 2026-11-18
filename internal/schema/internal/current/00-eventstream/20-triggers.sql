--------------------------------------------------------------------------------
-- The "signal_event_stream_created" trigger sends the "event_stream_created"
-- signal when event streams are created. source.Local in
-- internal/eventstream/source waits for it to discover new streams.
--
-- PostgreSQL delivers identical notifications sent within one transaction only
-- once, so a transaction that creates many streams produces a single signal.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION runkit.signal_event_stream_created()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('signal', 'event_stream_created') FROM inserted;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE TRIGGER signal_event_stream_created
AFTER INSERT ON runkit.event_streams
REFERENCING NEW TABLE AS inserted
FOR EACH STATEMENT
EXECUTE FUNCTION runkit.signal_event_stream_created();

--------------------------------------------------------------------------------
-- The "signal_events_appended" trigger sends the
-- "events_appended.<stream-id>" signal when events are appended to an event
-- stream. source.Local in internal/eventstream/source waits for each stream's
-- signal to deliver its new events.
--
-- PostgreSQL delivers identical notifications sent within one transaction only
-- once, so a transaction that appends many events to a stream produces a single
-- signal.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION runkit.signal_events_appended()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('signal', 'events_appended.' || stream_id) FROM inserted;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE TRIGGER signal_events_appended
AFTER INSERT ON runkit.events
REFERENCING NEW TABLE AS inserted
FOR EACH STATEMENT
EXECUTE FUNCTION runkit.signal_events_appended();
