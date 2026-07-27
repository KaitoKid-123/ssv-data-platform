# store_operation stream producer (realtime speed layer)

Synthetic producer for the **eod_sale_product realtime** flow. It emits
`SALE_TRANSACTION` events to **Aiven Kafka** (topic `store_operation`); a Fabric **Spark
Structured Streaming** job then builds the realtime fact — the Fabric equivalent of the
ClickHouse lambda flow:

```
ClickHouse (today):  Kafka store_operation → MV rlt_sale_product_mv → rlt_sale_product (RMT, TTL 72h)
                     → eod_sale_product_view_rlt (UNION: batch eod_sale_product + realtime rlt)

Fabric (target):     Kafka store_operation → Spark Structured Streaming (reuse ssv_data silver
                     transforms) → gold.rlt_fact_eod_sale_product (Delta, MERGE, ~72h) 
                     → view UNION fact_eod_sale_product(batch) + rlt(today) → Direct Lake
```

**This repo = the producer only** (the Kafka source). The Spark consumer is the next step.

## Message shape

A JSON envelope identical in structure to the real `data_kafka.store_operation_mt.payload`
(values are **synthetic** — no real customer/supplier data):

```jsonc
{ "stream_type": "SALE_TRANSACTION", "topic": "store_operation", "source": "1079",
  "created_at": 1785122631269,
  "payload": { "store_code": "1079", "store_name": "...",
    "sale_transaction": [{
      "transaction_id": "10790210056750001", "staff_id": "1005675", "document_date": 1785122631000,
      "payment_method": 0, "total_amount": 33000, "customer_gender": 1,
      "sale_normal_items": [{ "product_code": "96010001", "quantity": 2, "retail_selling_price": 12000,
        "product_group": "Soft Drinks", "product_category": "Beverage", "total_amount": 24000,
        "purchase_price_with_tax": 8800, "retail_business_type": "Z01 - Normal Merchandise", ... }] }] } }
```

`GET /sample` on either service returns a full generated envelope to inspect.

## Deploy A — Cloudflare Worker (recommended, always-on, no server)

Cloudflare Workers can't speak native Kafka, so this produces over **Aiven's Kafka REST
API (Karapace)** — an HTTP endpoint.

1. **Aiven**: on the Kafka service, enable **Kafka REST API** (Karapace) and create the
   topic `store_operation` (e.g. 3 partitions). Note the REST URI + a user/password.
2. **Deploy**:
   ```bash
   cd sample_stream
   npm i -g wrangler
   wrangler secret put KAFKA_REST_URL   # https://<svc>-<proj>.aivencloud.com:<rest-port>
   wrangler secret put KAFKA_USER       # e.g. avnadmin
   wrangler secret put KAFKA_PASS
   wrangler deploy
   ```
3. Cron (`wrangler.toml [triggers]`) produces `BATCH` msgs/minute automatically. Manual:
   ```bash
   curl -XPOST "https://<name>.<subdomain>.workers.dev/produce?n=50"
   ```

## Deploy B — container (native Kafka, no REST proxy)

For Cloud Run / Render / Fly / ACA. Uses `confluent-kafka` over SASL_SSL directly.

```bash
docker build -t so-producer sample_stream
docker run -e KAFKA_BOOTSTRAP=<host:port> -e KAFKA_USER=avnadmin -e KAFKA_PASS=... \
  -e KAFKA_CA=/app/ca.pem -e STREAM_INTERVAL_SEC=15 -p 7860:7860 so-producer
```
`STREAM_INTERVAL_SEC>0` → continuous producer; else produce on `POST /produce`.

## Config

| Var | Worker | Container | Meaning |
|---|---|---|---|
| `KAFKA_TOPIC` | var (default `store_operation`) | env | target topic |
| `BATCH` | var (default 10) | env | msgs per cron tick / default batch |
| `KAFKA_REST_URL`/`USER`/`PASS` | secret | — | Aiven Karapace REST + basic auth |
| `KAFKA_BOOTSTRAP`/`USER`/`PASS`/`CA`/`MECHANISM` | — | env | native SASL_SSL |
| `STREAM_INTERVAL_SEC` | — (cron instead) | env | auto-produce interval (container) |

Both services expose `GET /health`, `GET /sample`, `POST /produce?n=<k>`.
