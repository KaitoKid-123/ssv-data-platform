# ssv-data-platform

Production-style **medallion lakehouse** on Microsoft Fabric that migrates legacy
pandas + Airflow end-of-day (EOD) ETL jobs to Fabric-native OneLake Delta pipelines —
ingesting from **MongoDB, PostgreSQL and a REST API**, transforming through
**bronze → silver → gold**, and guarding output with a **data-quality gate**.

![CI](https://github.com/KaitoKid-123/ssv-data-platform/actions/workflows/ci.yml/badge.svg)

The shared `ssv_data` framework now powers **two** EOD pipelines that reuse the same
functional-core transforms, `MedallionPipeline` shell, DQ gate and CI/CD tooling:

| Pipeline | Sources | Gold fact | BI |
|---|---|---|---|
| **`eod_sale_product`** | Mongo (bills) + PostgreSQL + DLM REST | `fact_eod_sale_product` (~80 cols, sale-line grain) | model `Sales` + 6-page dashboard |
| **`eod_sale_service`** | Mongo only — `pay_bill_transaction` + `top_up_transaction` (Pay Bill / Pay Card / Top Up) | `fact_eod_sale_service` (57 cols, txn×item grain) | model `Sales Service` + 2-page dashboard |

Each lives in its own workspace folder (`ETL_Sales/pipelines/<name>`), is promoted
DEV→PROD by the same tooling, and adds only thin wiring notebooks — all reusable logic
graduates into the tested `ssv_data` wheel. `eod_sale_product` additionally has a
**realtime speed layer** (Kafka → Spark Structured Streaming → a Lambda union view) —
see [Realtime (speed layer)](#realtime-speed-layer--eod_sale_product) below.

The pipeline is deployed and verified end-to-end in Fabric against real sources
(MongoDB Atlas, Aiven PostgreSQL, a Cloudflare-Workers mock for the DLM API), and the
whole transform layer is unit- and integration-tested on a local SparkSession — no
Fabric needed to run the tests.

---

## What this demonstrates

- **Multi-source ingestion** with the right tool chosen per source (no-code Copy for Mongo, JDBC for Postgres, REST for the API).
- **Medallion architecture** (raw → typed/exploded → business fact) on Delta with **idempotent** partition writes.
- **Testability**: functional-core transforms are pure `df -> df`, covered by unit + e2e tests on local Spark.
- **Data quality as a gate**: a fail-loud check turns the pipeline red on bad data instead of publishing it.
- **Config over code**: a swappable secret resolver + per-source toggles → the same code runs local, dev and prod.
- **Realistic data**: a deterministic simulator mirrors the real source topology and generates a configurable date range for trend/MoM dashboards.

---

## Architecture

```mermaid
flowchart LR
  subgraph SRC [Sources]
    M[("MongoDB Atlas<br/>sale_bill · sale_return_bill<br/>partner_store")]
    P[("PostgreSQL / Aiven<br/>9 operational tables<br/>+ 3 dims")]
    D["DLM REST API<br/>partner_sources"]
  end
  subgraph BR [Bronze — raw Delta]
    B1["sale_bill / return<br/>nested array&lt;struct&gt; kept intact"]
    B2["partner_store (flat)"]
    B3["pg tables + dims"]
    B4["partner_sources"]
  end
  subgraph SI [Silver — typed &amp; exploded]
    S1["sale line items<br/>+7h VN day · from_json → explode<br/>dim_product / dim_store / …"]
  end
  subgraph GO [Gold — business fact]
    G1["fact_eod_sale_product<br/>~80 cols · replaceWhere(report_date)"]
  end
  M -- "Copy activity — windowed (no-code)" --> B1
  M -- "Copy activity — full (no-code)" --> B2
  P -- "JDBC / OneLake shortcut" --> B3
  D -- "requests" --> B4
  B1 --> S1
  B2 --> S1
  B3 --> S1
  B4 --> S1
  S1 --> G1
  G1 --> DQ{{"DQ gate — fail loud"}}
```

**Bronze** lands each source as raw Delta, one entrypoint per source. **Silver** shifts
`documentDate` by **+7h** to VN business day, parses the nested Mongo `saleNormalItems`
array (`from_json` → `explode`) into line items, and builds the conformed dims. **Gold**
joins promotions, delivery, price timeline and dims into an ~80-column fact at
**sale-line grain**, then writes with `replaceWhere(report_date)` so a re-run of any day
is idempotent and history accumulates one partition per day.

---

## Ingestion: choose the tool by data shape

The pipeline mixes **no-code (Copy) and code** ingestion, chosen per source — a concrete
demonstration that the right tool depends on the *shape* of the data:

| Source | Collection / tables | Tool | Note |
|---|---|---|---|
| MongoDB | `sale_bill`, `sale_return_bill` | **Copy activity** (no-code) | nested `array<struct>` → mapped as a **single JSON String** column |
| MongoDB | `partner_store` | **Copy activity** (no-code) | flat document, full load |
| PostgreSQL | 9 operational + 3 dims | **JDBC** (dev) / **OneLake shortcut** (prod) | relational, bulk |
| DLM | `partner_sources` | **`requests`** (code) | small REST payload |

> **Lesson learned — Copy flattens nested arrays unless you map them as one String.**
> By default the Copy mapping expands `saleNormalItems` into per-field columns
> (`saleNormalItems.productCode`, …) and writes **NULL** into the destination array —
> proven: `bronze.sale_bill` had rows but `count(saleNormalItems)=0`, so every `product_id`
> in gold came out null. The fix is to **collapse `saleNormalItems` / `transactionPromotions`
> to a single `String` column in the mapping** (and *not* re-run "Import schema", which
> re-expands them); Copy then serialises the whole array to a JSON string, which silver
> parses with `from_json` → `explode`. Takeaway: no-code *can* handle nested Mongo
> documents — you just have to understand how the connector maps arrays. (`bronze.py` also
> ships an equivalent Spark-connector path, `ingest_mongo`, if you prefer code.)
>
> **Refined by `eod_sale_service`:** collapsing a nested field to a `String` column is not
> enough on its own — MongoDbAtlasSource raises `JsonUnsupportedHierarchicalComplexValue`
> for a nested **object** (and array) unless you also set the translator flag
> **`mapComplexValuesToString: true`**, which serialises objects *and* arrays to JSON strings.

---

## Data-quality gate

`dq_check.py` (notebook `nb_dq_check` in the pipeline) validates the gold fact **for the run day** and raises
on any violation, so the pipeline activity goes red instead of silently publishing:
row count > 0, no null `transaction_id` / `product_id` / `report_date`, and
`final_amount` present for completed rows.

> **Lesson learned — scope DQ to the partition, not the whole table.** The first version
> asserted "gold has a *single* `report_date` == run day". That held when gold stored one
> day, but broke the moment gold began **accumulating history** via `replaceWhere`
> (`distinct=2` → failure). The fix: filter gold to `report_date == running_date` first,
> then check that slice. Validating the run-day partition is the correct scope for an
> incremental load.

---

## Design

**Functional core + thin OOP shell.** All transforms are pure functions (`df -> df`) that
run anywhere; the only objects are `PipelineContext` (spark + run window + secret resolver
+ logger) and `MedallionPipeline` (a Template-Method base owning run order, logging,
`backfill`, and idempotent writes). No object holds DataFrame state → everything is unit-testable.

**Idempotency.** `write_delta(..., replaceWhere=report_date)` replaces only the run's
partition, so any day can be safely re-run and a backfill just loops days.

**Config over code.** A single secret resolver is injected; swapping local Spark conf for
Azure Key Vault is a one-line change. Per-source toggles (`_opt(ctx, "pg-jdbc")`, etc.)
let the same `bronze_ingest` skip sources that arrive by shortcut in prod but by JDBC in dev.

---

## Repo layout

```
ssv_data/                     # SHARED library -> built as a wheel, attached to a Fabric Environment
  config.py                   # bronze/silver/gold names, VN tz offset (+7h)
  runtime/  context.py window.py pipeline.py logging.py   # PipelineContext + MedallionPipeline (Template Method)
  io/       readers.py writers.py                         # windowed/semi-join JDBC readers; write_delta(replaceWhere)
  schema/   registry.py cast.py                           # StructTypes; fill-missing + cast-by-schema
  transforms/ common.py scd.py # pure df->df: pivot/range-join/coalesce + SCD2 (scd2_apply, as_of_join)

sample_file/                  # PER-PIPELINE code — local copies of the Fabric notebooks (%run chain)
  bronze.py.ipynb             # per-source ingest: Mongo (windowed) / PG (windowed+semi-join/full) / DLM
  silver.py.ipynb gold.py.ipynb  # +7h day, explode items, SCD2 dims; ~80-col fact, as-of joins
  pipeline.py.ipynb dq_check.py.ipynb nb_bi_refresh.py.ipynb simulators.py.ipynb ...
  Pipeline_eod_sale_product/  # Fabric Data Pipeline definition (activities incl. nb_bi_refresh)
  create_report_pbir.py       # PBIR report generator (API-built dashboard pages)

fabric_items/                 # EXPORTED workspace definitions in git-integration layout —
                              #   both pipelines' notebooks/pipelines/TMDL/PBIR + manifest.json
                              #   + folders.json (workspace folder map) — backup, promotion & DR
tools/                        # fabric_api.py (SPN/az auth + LRO) · deploy_wheel.py ·
                              #   export_definitions.py · deploy_definitions.py (restore/DR/promote,
                              #   guid+ws-name remap, --folder domain scope) · verify_run.py
sample_service/               # DLM mock (FastAPI + Cloudflare Worker + Dockerfile)
sample_stream/                # realtime producer: store_operation events -> Aiven Kafka
                              #   (Cloudflare Worker via Kafka REST + confluent-kafka twin)
tests/                        # 35 unit + e2e tests on a local SparkSession (no Fabric)
docs/architecture/            # eod-sales-flow.drawio (4 pages) + rendered PNG previews
docs/superpowers/specs/       # design specs (windowed extraction, SCD2, dashboard extension)
docs/adr/                     # architecture decision records (ADR-0001: logic-in-notebooks trade-off)
.github/workflows/            # ci.yml (pytest+build) · deploy.yml (wheel -> Fabric via SPN)
```

### Architecture diagrams

Source: [docs/architecture/eod-sales-flow.drawio](docs/architecture/eod-sales-flow.drawio) (6 pages,
generated by [build_flow.mjs](docs/architecture/build_flow.mjs) — edit the script, not the boxes).

**1. Overview — one glance, for everyone**

![Overview](docs/architecture/eod-sales-flow-overview.png)

**2. Fabric platform map — where this sits inside Microsoft Fabric**

![Fabric platform map](docs/architecture/eod-sales-flow-platform.png)

**3. Data flow — medallion in detail (windowed ingest, SCD2, DQ gate, Direct Lake)**

![Data flow](docs/architecture/eod-sales-flow-data.png)

**4. Orchestration + CI/CD — pipeline DAG, GitHub lane, notebook lifecycle**

![Orchestration and CI/CD](docs/architecture/eod-sales-flow-orchestration.png)

**5. `eod_sale_service` — the second pipeline's medallion flow (Copy-fed, 57-col fact, Direct Lake)**

![eod_sale_service data flow](docs/architecture/eod-sales-flow-service.png)

**6. Realtime speed layer — `eod_sale_product` Lambda (Kafka → Spark Structured Streaming → union view)**

![Realtime speed layer](docs/architecture/eod-sales-flow-realtime.png)

---

## The `eod_sale` pipeline

- **Grain:** one row per sale line item.
- **Time:** source `documentDate` is UTC; a VN business day is `documentDate + 7h`. The run
  window derives `[utc_lo, utc_hi)` and Mongo reads are pushed-down on that window.
- **Customer id:** coalesce priority **flips on 2022-04-01** (handled explicitly in gold).
- **Cancellations:** delivery status 5/13 + `delivery_status='canceled'` are kept as canceled rows.
- **Cost/price:** `range_join_effective` joins the purchase-price timeline valid at transaction time.

---

## Second pipeline: `eod_sale_service`

Migrates a second legacy ETL (MongoDB → ClickHouse) for the payment services **Pay Bill /
Pay Card / Top Up**, landing a `gold.fact_eod_sale_service` fact that is **column-for-column
parity** with the legacy `hq_report.eod_sale_service` (57 columns). It reuses the whole
`ssv_data` framework and only adds thin `*_service` wiring notebooks + a Data Pipeline.

```mermaid
flowchart LR
  subgraph SRC [MongoDB]
    C1[("report.pay_bill_transaction<br/>Pay Bill")]
    C2[("report.top_up_transaction<br/>Top Up + Pay Card")]
  end
  subgraph BR [Bronze]
    B["pay_bill_transaction<br/>top_up_transaction<br/>orderInfo + items as JSON string"]
  end
  subgraph SI [Silver]
    S["sale_service_line<br/>explode items · parse orderInfo<br/>decode demographics"]
  end
  subgraph GO [Gold]
    G["fact_eod_sale_service<br/>57 cols · replaceWhere(report_date)"]
  end
  subgraph BI [BI]
    MT["bi_eod_sale_service mart<br/>→ Direct Lake 'Sales Service'"]
  end
  C1 -- "Copy — windowed (no-code)" --> B
  C2 -- "Copy — windowed (no-code)" --> B
  B --> S --> G --> DQ{{"DQ gate"}} --> MT
```

Design notes (things that differ from `eod_sale_product`):

- **Two collections, three services.** Mongo has only `pay_bill_transaction` and
  `top_up_transaction` — **Pay Card lives inside `top_up_transaction`** (discriminated by the
  document's `serviceName`, 84 % of rows). Silver reads 2 collections; `service_name` passes
  straight through, so one collection legitimately yields two service labels.
- **VN-local time.** `transactionTime` is already VN-local — `report_date` is a plain
  date-part, **no +7h shift** (contrast the product pipeline's UTC `documentDate`).
- **Copy-only ingest.** Bronze is fed exclusively by the pipeline's Copy activities; the
  pipeline's `ingest()` is a no-op (there is no notebook-side Mongo read to duplicate it).
- **Nested Mongo + Copy.** A nested **object** (`orderInfo`) *cannot* Copy as a JSON string
  by default (`JsonUnsupportedHierarchicalComplexValue`); the fix is translator
  **`mapComplexValuesToString: true`**, which serialises both the object and the item
  **array** to JSON strings that silver parses (dot-notation flattening of `orderInfo.<sub>`
  yields all-NULL columns — it does not work for MongoDbAtlasSource).
- **Grain** is `(transaction_id, product_code)` — DQ guards it for uniqueness.
- **Shared decode.** Gender / age / nationality decoding graduated into
  `ssv_data.transforms.demographics` (rule of two — used by both pipelines), with a
  `keep_unknown_age` flag for the service pipeline's pass-through behaviour.

Pipeline DAG: `Set variable → [cp_mongo_pay_bill ‖ cp_mongo_top_up] → nb_transform_service
(silver + gold + DQ) → nb_bi_refresh_service (rebuild mart + refresh Direct Lake model)`.

---

## Realtime (speed layer) — `eod_sale_product`

Alongside the daily batch fact, `eod_sale_product` has a **Lambda speed layer**: *past days
from batch, today from a live stream*, stitched by one view — a Fabric port of the ClickHouse
`store_operation → rlt_sale_product → eod_sale_product_view_rlt` flow. Chosen implementation:
**Spark Structured Streaming (micro-batch)** rather than Eventhouse/KQL, to reuse the medallion
+ transforms and keep costs to batch (only runs on a schedule).

```mermaid
flowchart LR
  W["Cloudflare Worker<br/>(cron 1&#39;)"] -- "Kafka REST" --> K[("Aiven Kafka<br/>store_operation")]
  K -- "readStream (SASL_SSL)" --> C["rlt_ingest_sale_product<br/>Trigger.AvailableNow (sched 5&#39;)<br/>parse envelope · explode items · +7h"]
  C -- "MERGE (72h TTL)" --> R["gold.rlt_fact_eod_sale_product<br/>(Delta, dedup txn+product+uom)"]
  F["gold.fact_eod_sale_product<br/>(batch)"] --> V{{"gold.vw_eod_sale_product_rlt<br/>UNION: batch ≤maxdate ⊎ rlt >maxdate"}}
  R --> V
  V --> M["gold.bi_eod_sale_product_rlt<br/>(7-day mart, refreshed each run)"]
  M -- "Direct Lake" --> D["model 'Sales Product Realtime'<br/>+ report (Auto Page Refresh 5&#39;)"]
```

- **Producer** (`sample_stream/`): a Cloudflare Worker emits synthetic `store_operation`
  SALE_TRANSACTION events (shape mirrors the real ClickHouse `store_operation_mt` payload) to
  Aiven Kafka via the Kafka REST proxy (Karapace); a `confluent-kafka` container twin exists too.
- **Consumer** (`rlt_ingest_sale_product.py`, scheduled every 5 min): `Trigger.AvailableNow`
  reads only new offsets (checkpointed → incremental), conforms to a subset of the batch fact's
  columns, **MERGE**-upserts into `gold.rlt_fact_eod_sale_product` (dedup on txn+product+uom),
  prunes rows older than 72h, rebuilds the UNION view and the Direct Lake mart.
- **Serving**: `gold.vw_eod_sale_product_rlt` = batch (`source='eod'`, `report_date ≤ max`)
  ⊎ realtime (`source='rlt'`, `report_date > max`); a Direct Lake model + report read the mart.
- **Refresh**: fully hands-off — Direct Lake auto-reframes off the mart Delta, and the report's
  **Automatic Page Refresh** re-queries every 5 min. End-to-end latency ≈ 5 min (micro-batch).
- The **CA cert** is a lakehouse file (`Files/aiven_ca.pem`); Kafka SASL creds are Environment
  Spark-conf secrets (`spark.ssv.secret.kafka_{bootstrap,user,pass}`).

---

## Run it locally

```bash
pip install -e ".[test]"     # pyspark + pytest + chispa
pytest -q                    # unit transforms + full e2e on local Spark
```

Seed synthetic data and run the pipeline with `with_ingest=False` (pure POC, no external sources):

```python
%run simulators.py
save_all_bronze(spark)                                   # one day -> bronze Delta
EodSalePipeline(spark=spark, schema_enabled=True).run("2025-11-17", with_ingest=False)

# multi-day for trend/MoM dashboards:
save_all_bronze(spark, start="2025-11-01", end="2025-11-30")
for d in _daterange("2025-11-01", "2025-11-30"):
    EodSalePipeline(spark=spark, schema_enabled=True).run(d, with_ingest=False)
```

The simulator keeps master data (products, stores, users) stable across days and generates
transactions **per day** with a date-derived seed and a weekday/weekend volume pattern, so
ids are globally unique across days and dashboards show real day-over-day trend.

---

## Deploy on Fabric

1. **Build & attach the wheel.** `python -m build --wheel` → upload to a **Custom Environment**
   (Libraries), publish, attach to the notebooks.
2. **Secrets** as Environment Spark conf (`spark.ssv.secret.mongo_conn`, `pg_jdbc`, `dlm_auth`,
   `dlm_url`); the resolver maps `-`→`_`. In production, swap the resolver for Key Vault
   (`notebookutils.credentials.getSecret`) — one line.
3. **Jars** in the Environment: `org.postgresql:postgresql:42.7.4` (for the PG JDBC ingest).
   Add `org.mongodb.spark:mongo-spark-connector_2.12:10.4.0` only if you use the optional
   code path (`ingest_mongo_bills`) instead of the Copy activities.
4. **Data pipeline** (`Pipeline_eod_sale_product`): `Set variable (v_run_date)` →
   `[cp_mongo_sale_bill ‖ cp_mongo_return ‖ cp_mongo_partner_store ‖ nb_ingest_pg ‖ nb_ingest_dlm]`
   → `nb_transform` → `nb_dq_check` → `nb_bi_refresh` (rebuilds the `bi_*` marts +
   `dim_date` the semantic model reads, so dashboards only refresh from data that
   passed the DQ gate). `run_date` empty → yesterday (VN); else an explicit
   date for backfill.
5. **Backfill** a range with a **ForEach (Sequential)** parent pipeline that invokes the
   pipeline once per day — sequential because bronze is overwritten per run while only gold
   is partition-safe.
6. **Schedule** daily ≥ 17:00 UTC; alert on failure.

### Environments

| | Bronze source | Secrets | Notes |
|---|---|---|---|
| **Local / test** | synthetic → bronze, or JDBC | local Spark conf | `pytest`, no Fabric |
| **Dev** | Atlas + Aiven PG + mock DLM | Spark conf | full multi-source extract |
| **Prod** | OneLake shortcut / Mirroring + connectors | **Azure Key Vault** | wheel from CI |

---

## Note on data

All source data here is **synthetic** (`simulators.py`) but structurally faithful to the
real system: nested Mongo documents, the relational + dim tables, the DLM payload, the
+7h day boundary and the customer-id rule. Swapping in a real source is a config change
(secret + toggle), not a code change.
## CI/CD & tooling

- **CI** (`.github/workflows/ci.yml`): pytest + wheel build on every PR/push.
- **CD** (`.github/workflows/deploy.yml`): manual button or `v*` tag → test → build →
  publish wheel to the Fabric `Custom_Env` (SPN secrets: `FABRIC_TENANT_ID/CLIENT_ID/CLIENT_SECRET`).
  Notebooks / semantic model / report are developed directly on Fabric (thin-shell rule:
  logic goes into `ssv_data` with tests, notebooks only wire and call).
- **`tools/`**: `deploy_wheel.py` (staging→publish), `export_definitions.py` (backup all
  workspace item definitions into `fabric_items/`), `verify_run.py` (idempotent pipeline
  run + DAX diff vs `tools/baseline_sales_daily.json` — synthetic data is deterministic,
  any drift = regression). Auth: SPN env vars, falling back to `az` CLI login.
- **Ops**: `MedallionPipeline.run()` appends step timings/status to `ops.run_log` (0.1.5);
  `.github/workflows/monitor.yml` (daily cron) opens a GitHub issue on any Failed run.
  Secrets stay as Fabric Environment Spark properties for now — Azure Key Vault needs an
  Azure subscription, which this tenant doesn't have yet.
- **`fabric_items/`**: exported definitions (notebooks/pipelines/TMDL/PBIR) + `manifest.json`
  (item ids for reference remapping) — the workspace's state in reviewable text form;
  re-export via `tools/export_definitions.py`.
- **Restore / disaster recovery / promotion**: `tools/deploy_definitions.py` pushes
  `fabric_items/` back — in-place updates on the same workspace, or into ANOTHER workspace
  (`--workspace <id>`) with automatic remapping of **GUIDs** (lakehouse/env/notebook/model)
  **and the workspace name** (reports embed it in their connection string). It also
  reconciles each item into its `folders.json` folder, and `--folder <path>` /  `--only` /
  `--item` scope a partial deploy (e.g. promote one domain).

### Promote DEV → PROD

Golden rule: **develop on DEV, never edit PROD directly** — then promotion is a safe sync.

```bash
# 1. Snapshot DEV into fabric_items/ (git source of truth) + manifest (DEV ids)
python tools/export_definitions.py --workspace <DEV_ID>

# 2. Deploy to PROD — remaps every GUID + the workspace name, places items in folders.
#    --folder scopes to one domain so untouched pipelines stay put.
python tools/deploy_definitions.py --workspace <PROD_ID> \
       --folder ETL_Sales/pipelines/eod_sale_service      # (--dry-run to preview)

# 3. If the wheel changed:
python tools/deploy_wheel.py --workspace <PROD_ID> --environment <PROD_ENV_ID>
```

> `export_definitions` **prunes** `fabric_items/` to mirror the exported workspace, so DEV
> must hold everything you promote (`folders.json`/`manifest.json`/`parameter.yml` are kept).
> Notebooks + semantic models + reports are built on DEV, then promoted — no direct PROD edits.
