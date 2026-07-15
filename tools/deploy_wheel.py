"""Deploy the ssv_data wheel to the Fabric Custom_Env: build -> staging -> publish.

Usage:  python tools/deploy_wheel.py [--no-build]
Auth:   SPN env vars (CI) or az CLI login (local) — see fabric_api.token().
"""
import glob
import os
import subprocess
import sys
import time

import requests

sys.path.insert(0, os.path.dirname(__file__))
from fabric_api import BASE, ENV_ID, WS, _h, call

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def main(build: bool = True) -> None:
    if build:
        subprocess.run([sys.executable, "-m", "build", "--wheel"], cwd=ROOT, check=True)
    wheels = sorted(glob.glob(f"{ROOT}/dist/ssv_data-*.whl"), key=os.path.getmtime)
    wheel = wheels[-1]
    name = os.path.basename(wheel)
    print(f"deploying {name}")

    staging = f"/workspaces/{WS}/environments/{ENV_ID}/staging/libraries"
    with open(wheel, "rb") as f:
        call("POST", staging, files={"file": (name, f)})

    # keep exactly one ssv_data wheel in staging
    current = call("GET", staging).json()["customLibraries"]["wheelFiles"]
    for old in current:
        if old.startswith("ssv_data-") and old != name:
            call("DELETE", f"{staging}?libraryToDelete={old}")
            print(f"removed old {old}")

    call("POST", f"/workspaces/{WS}/environments/{ENV_ID}/staging/publish")
    print("publish started; polling...")
    for _ in range(120):                      # up to ~30 min
        time.sleep(15)
        st = call("GET", f"/workspaces/{WS}/environments/{ENV_ID}").json() \
            ["properties"]["publishDetails"]["state"]
        print("  publish:", st)
        if st.lower() == "success":
            live = call("GET", f"/workspaces/{WS}/environments/{ENV_ID}/libraries") \
                .json()["customLibraries"]["wheelFiles"]
            print("published wheels:", live)
            return
        if st.lower() in ("failed", "cancelled"):
            raise SystemExit(f"publish {st}")
    raise SystemExit("publish timed out")


if __name__ == "__main__":
    main(build="--no-build" not in sys.argv)
