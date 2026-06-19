CREATE SCHEMA IF NOT EXISTS common;

--------------------------------------------------------------------------------
-- The "exponential_backoff" function computes a capped exponential backoff
-- interval with equal jitter from an iteration count and base delay.
--
-- The result is a random interval between "base" and min(cap, base * 2^n).
-- This ensures a minimum delay of "base" while spreading retry attempts across
-- the backoff window to reduce contention.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION common.exponential_backoff(
    iterations bigint,
    base       interval,
    cap        interval
)
RETURNS interval
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    -- The multiplier doubles with each iteration, producing the exponential
    -- backoff curve.
    multiplier float8 := pow(2, exponential_backoff.iterations);

    -- The maximum multiplier that can be applied to the base interval without
    -- exceeding the cap. This is computed in the float domain, where very large
    -- values and infinity are safe.
    max_multiplier float8 := extract(epoch FROM exponential_backoff.cap) / extract(epoch FROM exponential_backoff.base);

    -- The upper bound of the jitter window. The multiplier is capped before
    -- being applied to prevent int64 microsecond overflow.
    ceiling interval := exponential_backoff.base * LEAST(multiplier, max_multiplier);
BEGIN
    -- Return a random interval between base and the ceiling (equal jitter).
    --
    -- This is a linear interpolation: base + t * (ceiling - base), where t is
    -- a random value in [0, 1).
    RETURN exponential_backoff.base + random() * (ceiling - exponential_backoff.base);
END;
$$;
