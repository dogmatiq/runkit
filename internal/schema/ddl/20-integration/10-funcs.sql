--------------------------------------------------------------------------------
-- The "complete_without_events" function removes a command from the queue when
-- no events were recorded during handling.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION integration.complete_without_events(
    message_id uuid
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM commandqueue.remove(complete_without_events.message_id);
END;
$$;

--------------------------------------------------------------------------------
-- The "complete_with_events" function removes a command from the queue and
-- appends events to an event stream in a single operation.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION integration.complete_with_events(
    message_id     uuid,
    correlation_id uuid,
    events         eventstream.event[]
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM commandqueue.remove(complete_with_events.message_id);

    PERFORM eventstream.append_any(
        complete_with_events.correlation_id,
        NULL,
        NULL,
        complete_with_events.events
    );
END;
$$;
