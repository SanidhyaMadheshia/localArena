# ADR-0022: Operational readiness: game days, alert coverage per layer, lab-only switches

- Status: accepted
- Date: 2026-10-19

## Context

The final game day (PM-002) showed that every layer needs its own "is it working" signal. The SLO alerts caught the user-facing faults, but a stopped stream processor and a silent business regression (no rewards) went unnoticed.

## Decision

- **Alert coverage per layer.** Each pipeline stage gets at least one cause alert:

  | Layer | Alert |
  |---|---|
  | gameplay | ticks, `TargetDown` |
  | API | burn rates |
  | DB tier | pool, PgBouncer waiting, replica lag, WAL retention |
  | events | spool, dropped |
  | processors | consumer lag growing |
  | business outcome | no rewards granted while gameplay flows |

  Every alert has a runbook and a promtool test.
- **Business-outcome alerts:** at least one alert asserts the product works end to end ("players who finish matches get credits"), not only that components are up.
- **Lab-only switches are explicit and off by default** (`LEGACY_V1`, `ALLOW_GUESTS`, `TRUST_PLAYER_PARAM`). Every switch that weakens security is named in the threat model and has a test for its default.
- **Game days:** after each release train, run randomized, sealed, overlapping faults under sustained load (`experiments/008-week8-regions/gameday.sh`). Score detection and mitigation time per fault, and turn findings into alerts or tests.
- **Mitigations come from runbooks and `make` targets** that carry the full configuration. Ad-hoc commands recreated a container with different settings during the game day.

## Consequences

- More alerts to maintain. The promtool tests and ticket/page severities keep the noise in check.
- Regressions such as the week 5 one surface within 15 minutes instead of three weeks.
