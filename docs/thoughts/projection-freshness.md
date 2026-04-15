# Projection freshness: estimating and describing how up to date a projection is

## Core idea

"Up to date" is not one number. It is a small vector that combines:

- position lag (how far behind in stream offsets)
- time lag (how old the newest applied source event is)
- coverage (whether all required streams have reached that point)
- confidence (whether the estimate is exact or inferred)

## Freshness tuple

For a projection `P`, expose this tuple:

`freshness(P) = (offset_lag, time_lag, coverage, confidence, as_of)`

Where:

- `offset_lag`: difference between source high-watermark and projection checkpoint.
- `time_lag`: `now - event_time_of_projection_checkpoint` (or ingest time if event time is unavailable/untrusted).
- `coverage`: fraction or set form, such as `12/12 streams current` or `missing: [3, 9]`.
- `confidence`: one of `exact`, `bounded`, `estimated`.
- `as_of`: timestamp when the estimate was computed.

## Estimation mechanics

1. Maintain source high-watermarks per stream.
2. Maintain projection checkpoints per stream.
3. Compute per-stream lag.
4. Aggregate using:

- max lag (worst-case safety)
- p95 lag (operational signal)
- mean lag (trend only)

Use max lag for correctness-sensitive language like "fully caught up".

## Describing freshness to users

Present both numbers and states:

- `current`: all streams at high-watermark.
- `near-real-time`: max time lag <= SLO budget (for example <= 5s).
- `delayed`: lag above budget but progressing.
- `stalled`: no checkpoint movement for timeout window.
- `degraded`: estimate only (for example, missing source watermark for one or more streams).

A useful status line shape:

`near-real-time (max 2.1s, p95 0.8s, 12/12 streams, exact, as of 2026-04-10T12:34:56Z)`

## Practical notes

- Prefer ingest time for SLOs; event time can be skewed by producer clocks.
- Keep both wall-clock lag and offset lag; each catches different failure modes.
- Always include `as_of` so readers can detect stale metrics.
- Distinguish "no new data" from "cannot progress". Zero lag while idle is healthy.
- For historical replays, report against replay target watermark, not live tail.

## Open questions

- Should freshness be defined against stream tail, or against a per-consumer commit frontier?
- Do we need monotonic confidence transitions (`exact -> bounded -> estimated`) in outages?
- What is the canonical freshness SLO per projection type?

## Possibly related

- [Event stream model ADR](../adr/0010-event-streams.md)
- [Event stream implementation notes](./event-stream-implementation.md)
- [Big picture thought](./000-big-picture.md)
