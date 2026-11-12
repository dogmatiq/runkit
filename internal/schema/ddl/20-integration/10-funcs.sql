-- A database created by 0.2.0 has these functions, and nothing calls them.
DROP FUNCTION IF EXISTS integration.complete_without_events(uuid);
DROP FUNCTION IF EXISTS integration.complete_with_events(uuid, uuid, eventstream.event[]);
