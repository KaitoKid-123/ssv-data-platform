"""Post-deploy verification: run the pipeline for an idempotent day, then diff the
dashboard-facing numbers against the committed baseline (30 synthetic days).

The synthetic data is deterministic, so ANY numeric drift = regression.
This is the automated version of the manual snapshot-diff used to verify 0.1.3/0.1.4.

Usage:  python tools/verify_run.py [run_date]      # default 2025-11-29
"""
import json
import os
import sys
import time

import requests

sys.path.insert(0, os.path.dirname(__file__))
from fabric_api import BASE, PIPELINE_ID, WS, _h, dax

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASELINE = os.path.join(os.path.dirname(__file__), "baseline_sales_daily.json")

DAX_DAILY = ('EVALUATE SUMMARIZECOLUMNS(\'Sales Daily\'[report_date], '
             '"rev_gross", SUM(\'Sales Daily\'[revenue_gross]), '
             '"rev_net", SUM(\'Sales Daily\'[revenue_net]), '
             '"baskets", SUM(\'Sales Daily\'[baskets]), '
             '"units", SUM(\'Sales Daily\'[units])) '
             'ORDER BY \'Sales Daily\'[report_date]')


def run_pipeline(run_date: str) -> None:
    r = requests.post(
        f"{BASE}/workspaces/{WS}/items/{PIPELINE_ID}/jobs/instances?jobType=Pipeline",
        headers={**_h(), "Content-Type": "application/json"},
        json={"executionData": {"parameters": {"run_date": run_date}}})
    r.raise_for_status()
    loc = r.headers["Location"]
    print(f"pipeline triggered for {run_date}")
    for _ in range(90):                       # ~45 min budget; runs take ~15-20 min
        time.sleep(30)
        j = requests.get(loc, headers=_h()).json()
        st = j.get("status")
        print("  job:", st)
        if st == "Completed":
            return
        if st in ("Failed", "Cancelled", "Deduped"):
            raise SystemExit(f"pipeline {st}: {json.dumps(j.get('failureReason', {}))[:500]}")
    raise SystemExit("pipeline poll timed out")


def snapshot() -> dict:
    rows = dax(DAX_DAILY)
    return {r["Sales Daily[report_date]"][:10]:
            [r["[rev_gross]"], r["[rev_net]"], r["[baskets]"], r["[units]"]] for r in rows}


def main() -> None:
    run_date = sys.argv[1] if len(sys.argv) > 1 else "2025-11-29"
    run_pipeline(run_date)

    after = snapshot()
    base = json.load(open(BASELINE))
    diffs = []
    for day in sorted(set(base) | set(after)):
        b, a = base.get(day), after.get(day)
        # daily marts round per day/channel -> allow tiny rounding noise on revenue
        if b is None or a is None or any(abs(x - y) > 10 for x, y in zip(b, a)):
            diffs.append((day, b, a))
    print(f"{len(base)} baseline days vs {len(after)} current: {len(diffs)} diffs")
    for d in diffs:
        print("  DIFF", d)
    if diffs:
        raise SystemExit("VERIFY FAILED — numbers drifted from baseline")
    print("VERIFY OK — dashboard numbers match the baseline")


if __name__ == "__main__":
    main()
