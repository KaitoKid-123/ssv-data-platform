"""Mock DLM REST service — mirrors
    https://dlm.sevensystem.vn/api/hq/dlm/partner_sources
so bronze_ingest can extract partner_sources from a LOCAL endpoint.

The response is a flat JSON array (list of objects), exactly what
`spark.createDataFrame(resp.json())` in bronze_ingest expects.

Run:
    pip install fastapi "uvicorn[standard]"
    uvicorn api_service:app --host 0.0.0.0 --port 8000

Point the pipeline at it (dev) by setting the secret:
    dlm-url = http://<host>:8000/api/hq/dlm/partner_sources
(In Fabric: spark.ssv.secret.dlm_url = http://<reachable-host>:8000/...)

Optional token check: set env DLM_TOKEN to require
    Authorization: Bearer <DLM_TOKEN>
Leave it unset to accept any/no Authorization header (easiest for local runs).
"""
import os

from fastapi import FastAPI, Header, HTTPException

app = FastAPI(title="DLM mock", version="1.0.0")

# Same rows the real endpoint returns. Keep in sync with
# simulators.api_partner_sources(); partner_source values match partner_store.partner_source.
PARTNER_SOURCES = [
    {"partner_source": "src1", "name": "GrabExpress"},
    {"partner_source": "src2", "name": "AhaMove"},
]

_TOKEN = os.getenv("DLM_TOKEN")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/hq/dlm/partner_sources")
def partner_sources(authorization: str | None = Header(default=None)):
    if _TOKEN and authorization != f"Bearer {_TOKEN}":
        raise HTTPException(status_code=401, detail="invalid token")
    return PARTNER_SOURCES
