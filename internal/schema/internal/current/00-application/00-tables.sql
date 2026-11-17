CREATE SCHEMA IF NOT EXISTS application;

--------------------------------------------------------------------------------
-- The "identity" table contains the identity of the application that the
-- database is bound to.
--
-- The "singleton" column ensures that the table contains at most one row.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS application.identity (
    singleton boolean PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    key       uuid    NOT NULL,
    name      text    NOT NULL CHECK (name != '')
);
