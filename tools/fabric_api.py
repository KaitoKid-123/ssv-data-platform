"""Fabric REST helper: auth (SPN or az CLI fallback) + long-running-operation polling.

Auth resolution order:
  1. Service Principal via env vars  FABRIC_TENANT_ID / FABRIC_CLIENT_ID / FABRIC_CLIENT_SECRET
     (the CI path — client-credentials flow, no interactive login)
  2. `az` CLI token of the logged-in user (the local-dev path)

Workspace constants are for the RetailSales_Analysis POC workspace.
"""
import base64
import json
import os
import subprocess
import time

import requests

BASE = "https://api.fabric.microsoft.com/v1"
FABRIC_RESOURCE = "https://api.fabric.microsoft.com"
POWERBI_RESOURCE = "https://analysis.windows.net/powerbi/api"

WS = "56f47ab7-4707-4862-9a9f-f8c05b6b4d63"          # RetailSales_Analysis
ENV_ID = "e31708f0-df03-4b50-94cf-602fc36d0525"      # Custom_Env
PIPELINE_ID = "27ac6611-66ec-40fe-b981-6249fe5a62f9" # Pipeline_eod_sale_product
DATASET_ID = "96783bbe-02d2-4ccd-9b66-15df675f631c"  # Semantic model 'Sales'

_tokens: dict = {}


def token(resource: str = FABRIC_RESOURCE) -> str:
    if resource in _tokens:
        return _tokens[resource]
    tenant = os.environ.get("FABRIC_TENANT_ID")
    client = os.environ.get("FABRIC_CLIENT_ID")
    secret = os.environ.get("FABRIC_CLIENT_SECRET")
    if tenant and client and secret:
        r = requests.post(
            f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token",
            data={"grant_type": "client_credentials", "client_id": client,
                  "client_secret": secret, "scope": f"{resource}/.default"})
        r.raise_for_status()
        _tokens[resource] = r.json()["access_token"]
    else:
        _tokens[resource] = subprocess.check_output(
            ["az", "account", "get-access-token", "--resource", resource,
             "--query", "accessToken", "-o", "tsv"], text=True).strip()
    return _tokens[resource]


def _h(resource=FABRIC_RESOURCE):
    return {"Authorization": f"Bearer {token(resource)}"}


def call(method: str, path: str, expect_lro_result: bool = False, **kw) -> requests.Response:
    """Request against the Fabric API; polls 202 long-running operations to completion."""
    r = requests.request(method, BASE + path, headers={**_h(), **kw.pop("headers", {})}, **kw)
    if r.status_code != 202:
        r.raise_for_status()
        return r
    loc = r.headers["Location"]
    for _ in range(240):                      # up to ~20 min
        time.sleep(int(r.headers.get("Retry-After", "5")))
        p = requests.get(loc, headers=_h())
        p.raise_for_status()
        st = p.json().get("status")
        if st == "Succeeded":
            if expect_lro_result:
                res = requests.get(loc + "/result", headers=_h())
                res.raise_for_status()
                return res
            return p
        if st in ("Failed", "Cancelled"):
            raise RuntimeError(f"LRO {st}: {p.text[:500]}")
    raise TimeoutError(f"LRO did not finish: {loc}")


def get_definition(item_type_path: str, item_id: str, fmt: str | None = None) -> list[dict]:
    """Item definition parts [{path, payload(b64), payloadType}]. item_type_path e.g. 'notebooks'."""
    q = f"?format={fmt}" if fmt else ""
    r = call("POST", f"/workspaces/{WS}/{item_type_path}/{item_id}/getDefinition{q}",
             expect_lro_result=True)
    return r.json()["definition"]["parts"]


def decode(part: dict) -> bytes:
    return base64.b64decode(part["payload"])


def list_items(ws: str = WS) -> list[dict]:
    return call("GET", f"/workspaces/{ws}/items").json()["value"]


def dax(query: str, dataset: str = DATASET_ID, group: str = WS) -> list[dict]:
    """Run one DAX query against the semantic model (Power BI executeQueries)."""
    r = requests.post(
        f"https://api.powerbi.com/v1.0/myorg/groups/{group}/datasets/{dataset}/executeQueries",
        headers={**_h(POWERBI_RESOURCE), "Content-Type": "application/json"},
        json={"queries": [{"query": query}]})
    r.raise_for_status()
    return r.json()["results"][0]["tables"][0]["rows"]
