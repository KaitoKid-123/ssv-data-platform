# DEV → PROD promotion flow

**Round-trip demonstrated & verified: 2026-07-16** (change made in DEV Notebook_test
→ exported → promoted → confirmed live in PROD).

Two workspaces on the same capacity:

| | Workspace | Purpose |
|---|---|---|
| DEV | `RetailSales_Analysis_DEV` `b90465fb-…` | edit notebooks/model freely on the UI; synthetic data |
| PROD | `RetailSales_Analysis` `56f47ab7-…` | dashboards people look at; changes arrive only by promotion (or wheel CD) |

## The loop

```bash
# 1. Dev on the Fabric UI in DEV (notebooks, model, report). Run/test there.

# 2. Capture DEV state into git
python tools/export_definitions.py --workspace b90465fb-bc4c-4a8b-a5bc-3d5eb2991337
git diff            # review: real content changes vs GUID noise (see below)

# 3. Promote selectively to PROD (deploy_definitions matches items by NAME and
#    remaps every DEV GUID -> PROD GUID via fabric_items/manifest.json)
python tools/deploy_definitions.py --item <displayName>     # or --only Notebook

# 4. Restore the canonical PROD mirror in git (also captures what was promoted)
python tools/export_definitions.py
git add fabric_items && git commit
```

Wheel (`ssv_data`) is NOT promoted this way — it releases to PROD via git tag → CD,
and to DEV via `tools/deploy_wheel.py --workspace <dev> --environment <dev-env>`.

## Reading the diff after a DEV export

Two kinds of changes show up — learn to tell them apart:
- **Real content**: cell sources, TMDL measures, PBIR visuals → what you review.
- **GUID re-binding noise**: `manifest.json` ids, notebook `dependencies` (lakehouse/env
  attachments), the DirectLake URL in `expressions.tmdl`, `definition.pbir` model id —
  these differ per-workspace by nature and are what `deploy_definitions` remaps.
  Never hand-edit them.

## Rules of thumb

- Promote by `--item` (surgical). A full un-filtered deploy from a DEV manifest would
  also CREATE DEV-only scratch items (e.g. `drill_seed`) in PROD.
- PROD stays hands-off: humans edit DEV; PROD changes = promotion + wheel CD + daily runs.
- After promoting, always re-export PROD so `fabric_items/` in git remains the PROD mirror.
