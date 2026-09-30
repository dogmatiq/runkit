CREATE SCHEMA IF NOT EXISTS cluster;

--------------------------------------------------------------------------------
-- The "heartbeat" table stores the last heartbeat timestamp for each node in
-- the cluster.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cluster.heartbeats (
    node_id    uuid    PRIMARY KEY,
    expires_at timestamptz NOT NULL
);
