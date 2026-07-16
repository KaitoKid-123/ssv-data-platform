# ADR-0001: Pipeline-specific logic (silver/gold) stays in Fabric notebooks

**Date:** 2026-07-16 · **Status:** Accepted · **Decider:** project owner

## Context

`silver.py` / `gold.py` notebooks hold the business-critical transform logic for
`eod_sale_product` (~400 lines: identity coalesce with the 2022-04-01 priority flip,
canceled-order branch, channel/payment decoding, fact assembly). This code is not
covered by pytest because it lives in notebook cells, not in an importable package.

Two packaging options were evaluated:
1. Move the logic into the wheel under a per-pipeline namespace
   (`ssv_pipelines/eod_sale_product/`), keeping `ssv_data` purely shared.
2. Keep the logic in notebooks (status quo).

## Decision

**Keep the logic in notebooks (option 2).**

Rationale: the project is developed solo with a UI-first workflow on Fabric. Moving
logic into the wheel changes the edit-run loop from seconds (edit cell → run) to
minutes (build → upload → publish ~5 min → restart session), and moves day-to-day
debugging (`display()` on intermediates, stack traces into cells) away from where
development actually happens. The testability gain does not outweigh that friction
at the current team size and pipeline count.

## Consequences & compensating controls

- The heaviest business logic has no unit tests. Compensating controls:
  - **Deterministic verification**: synthetic data is deterministic, so
    `tools/verify_run.py` (pipeline run on an idempotent day + DAX diff against the
    committed 30-day baseline) acts as an end-to-end regression test on Fabric.
  - **DQ gate** (`nb_dq_check`) fails the pipeline loudly on bad output; grain
    uniqueness check to be added (see review P0).
  - **Backups with history**: `tools/export_definitions.py` → `fabric_items/` in git
    gives notebooks diffable history and a restore path (`deploy_definitions.py`).
- Shared, generic logic must still graduate into `ssv_data` (tested) when it emerges —
  the "thin-shell" rule stays the goal for NEW shared code (rule of two).

## Revisit when

- A second pipeline (e.g. `eod_sale_service`) starts copy-pasting silver/gold code, or
- more than one person edits the notebooks, or
- a notebook-logic regression ships to the dashboard that a unit test would have caught.

Then re-evaluate option 1 (same wheel, `ssv_pipelines/<pipeline>/` namespace; notebooks
shrink to wiring), using session-scoped `%pip install` from Lakehouse Files and the
"shadow override" cell pattern to keep Fabric iteration fast.
