# Disaster-recovery runbook — rebuild the workspace from git

**Drill executed & PASSED: 2026-07-16** — empty workspace → full rebuild → numbers
matched the committed baseline exactly (2025-11-17: gross 43,330,800 · net 39,391,636 ·
226 baskets · 1,290 units). Total wall time ≈ 45 min, mostly Fabric job/publish waits.

## Steps (all from this repo, az login or SPN env vars)

```bash
# 0. Create the target workspace on a capacity (or reuse one)
#    POST /v1/workspaces {displayName, capacityId}

# 1. Restore all items — creates Lakehouse (schema-enabled) + Environment,
#    then notebooks -> model -> report -> pipelines, remapping every GUID
python tools/deploy_definitions.py --workspace <new-ws-id>

# 2. Wheel into the NEW environment (id printed by step 1)
python tools/deploy_wheel.py --no-build --workspace <new-ws-id> --environment <new-env-id>

# 3. Data: synthetic path — run a seed notebook (save_all_bronze) then the transform chain
#    (drill used: drill_seed -> main.py(running_date) -> nb_dq_check -> nb_bi_refresh)
#    Real-source path: set spark.ssv.secret.* on the Environment, re-link the Mongo
#    connection on the Copy activities, then run Pipeline_backfill_eod.

# 4. First query needs a model reframe:
#    POST /v1.0/myorg/groups/<ws>/datasets/<model>/refreshes
#    then verify with tools/verify_run.py-style DAX diff vs tools/baseline_sales_daily.json
```

## Gotchas learned in the drill

1. **Module notebooks don't carry a lakehouse attachment.** Notebooks that are only
   `%run` (simulators, bronze/silver/gold) have no `dependencies.lakehouse` — by design.
   Driver notebooks (main.py, nb_dq_check, nb_bi_refresh) do, and the restore remaps
   them correctly. If you author a NEW driver notebook, copy metadata from `main.py`,
   not from a module notebook, or Spark SQL has no default lakehouse and the session dies.
2. **A restored Direct Lake model returns HTTP 400 on executeQueries until its first
   refresh** (never framed). Kick one refresh, then query.
3. **Run order matters**: environment publish must FINISH before running any notebook
   attached to it (wheel import fails / session flakiness mid-publish).

## Not restored automatically (by design)

- Lakehouse DATA — rebuild via simulators seed + backfill (deterministic), or real sources.
- Environment Spark properties (`spark.ssv.secret.*`) — set per environment.
- Mongo CONNECTION on Copy activities — tenant-level resource; re-link in pipeline UI once.
- Schedules — re-create on the pipeline.

## Current DEV workspace (kept from the drill)

`RetailSales_Analysis_DEV` = `b90465fb-bc4c-4a8b-a5bc-3d5eb2991337`
(lakehouse `c2f2aef9-…`, env `1708163a-…`, model `8a9e6963-…`) — usable as the DEV half
of a DEV/PROD split; it already holds one verified day (2025-11-17) of synthetic data.
