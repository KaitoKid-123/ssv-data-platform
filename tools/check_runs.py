"""Monitor: list recent FAILED Fabric job runs for the EOD pipelines.

Writes failures.txt and exits 1 if anything failed in the window — the monitor
workflow turns that into a GitHub issue (email notification for free).

Usage:  python tools/check_runs.py [--hours 26]
Auth:   SPN env vars (CI) or az CLI login (local).
"""
import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(__file__))
from fabric_api import WS, call

WATCH = {
    "Pipeline_eod_sale_product": "27ac6611-66ec-40fe-b981-6249fe5a62f9",
    "Pipeline_backfill_eod": "6f258da5-f87e-4eb0-9eb6-4524f47c42d3",
}
BAD = {"Failed", "Cancelled"}


def parse_ts(ts: str | None):
    if not ts:
        return None
    return datetime.fromisoformat(ts[:26]).replace(tzinfo=timezone.utc)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=26)
    args = ap.parse_args()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=args.hours)

    failures, checked = [], 0
    for name, item_id in WATCH.items():
        runs = call("GET", f"/workspaces/{WS}/items/{item_id}/jobs/instances").json()["value"]
        for r in runs:
            started = parse_ts(r.get("startTimeUtc"))
            if not started or started < cutoff:
                continue
            checked += 1
            if r["status"] in BAD:
                reason = (r.get("failureReason") or {}).get("message", "")[:300]
                failures.append(f"- **{name}** run `{r['id']}` started {started:%Y-%m-%d %H:%M} UTC "
                                f"→ **{r['status']}**\n  {reason}")

    print(f"checked {checked} run(s) in the last {args.hours}h — {len(failures)} failure(s)")
    if failures:
        body = (f"Fabric run failures in the last {args.hours}h "
                f"(workspace RetailSales_Analysis):\n\n" + "\n".join(failures)
                + "\n\n_Reported by `.github/workflows/monitor.yml`._")
        with open("failures.txt", "w") as f:
            f.write(body)
        print(body)
        sys.exit(1)


if __name__ == "__main__":
    main()
