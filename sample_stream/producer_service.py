"""Synthetic `store_operation` Kafka producer — container twin of producer_worker.js.

Emits SALE_TRANSACTION events (same shape as the real ClickHouse
data_kafka.store_operation_mt payload, synthetic values) to Aiven Kafka using the
NATIVE Kafka protocol (confluent-kafka, SASL_SSL) — use this when you host on a
container platform (Cloud Run / Render / Fly / ACA) instead of Cloudflare Workers.
(The Cloudflare Worker uses Aiven's Kafka REST proxy because Workers can't do native
Kafka; this twin talks Kafka directly, no REST proxy needed.)

Env (Aiven Kafka service → "Connection information"):
    KAFKA_BOOTSTRAP     host:port  (SASL_SSL listener)
    KAFKA_USER          e.g. avnadmin
    KAFKA_PASS
    KAFKA_CA            path to the Aiven CA cert (ca.pem)   [optional if system trust ok]
    KAFKA_MECHANISM     SCRAM-SHA-256 (default) | SCRAM-SHA-512 | PLAIN
    KAFKA_TOPIC         store_operation (default)
    STREAM_INTERVAL_SEC if set (>0), auto-produce BATCH msgs every N seconds
    BATCH               messages per tick / default /produce batch (default 10)

Run:
    pip install -r requirements.txt
    uvicorn producer_service:app --host 0.0.0.0 --port 8000
    curl -XPOST "localhost:8000/produce?n=20"
"""
import json
import os
import random
import threading
import time
import uuid

from fastapi import FastAPI

STORES = [("1079", "P4-SH.07 VCP BTH", "107902"), ("1031", "Millenium Masteri D4", "103101"),
          ("1164", "246 Bui Vien D1", "116401"), ("1078", "S5.01-SH10 VGP THD", "107801")]
STAFF = [("1005675", "Nhan Vien A"), ("1010277", "Nhan Vien B"),
         ("1012047", "Nhan Vien C"), ("1029597", "Nhan Vien D")]
PRODUCTS = [
    dict(code="96010001", pid=10001, name="Coca-Cola 330ml", uom="CAN", grp_id=21, grp="Soft Drinks",
         sub_id=211, sub="Carbonated", cat_id=2000, cat="Beverage", biz="Z01 - Normal Merchandise",
         sup="3900001", sup_name="SUPPLIER ALPHA", vat="O3", price=12000, cost=8000),
    dict(code="96010002", pid=10002, name="Snack Khoai Tay 55g", uom="BAG", grp_id=11, grp="Snacks",
         sub_id=111, sub="Chips", cat_id=1000, cat="Food", biz="Z01 - Normal Merchandise",
         sup="3900002", sup_name="SUPPLIER BETA", vat="O3", price=18000, cost=12500),
    dict(code="86019244", pid=19244, name="Bang Keo OPP 40 Yard", uom="RO", grp_id=48, grp="Stationery",
         sub_id=984, sub="Packing and Adhesives", cat_id=176, cat="Office Supplies", biz="Z01 - Normal Merchandise",
         sup="3911170", sup_name="SUPPLIER GAMMA", vat="O3", price=15000, cost=9112),
    dict(code="96010004", pid=10004, name="Nuoc Suoi 500ml", uom="BOT", grp_id=22, grp="Water",
         sub_id=221, sub="Still Water", cat_id=2000, cat="Beverage", biz="Z01 - Normal Merchandise",
         sup="3900004", sup_name="SUPPLIER DELTA", vat="O3", price=8000, cost=5200),
]
TOPIC = os.getenv("KAFKA_TOPIC", "store_operation")
BATCH = int(os.getenv("BATCH", "10"))


def _now_ms():
    return int(time.time() * 1000)


def _item(p, qty):
    total = p["price"] * qty
    return {
        "product_code": p["code"], "product_id": p["pid"], "product_uom_id": 24000 + p["pid"] % 1000,
        "product_name": p["name"], "uom": p["uom"], "uom_size": 1, "quantity": qty,
        "retail_selling_price": p["price"],
        "product_group_id": p["grp_id"], "product_group": p["grp"],
        "product_sub_category_id": p["sub_id"], "product_sub_category": p["sub"],
        "product_category_id": p["cat_id"], "product_category": p["cat"],
        "retail_business_type": p["biz"], "supplier_code": p["sup"], "supplier_name": p["sup_name"],
        "output_vat_code": p["vat"], "purchase_price_without_tax": p["cost"],
        "purchase_price_with_tax": round(p["cost"] * 1.08),
        "total_amount_without_tax": round(total / 1.1), "total_amount": total, "sub_total": total,
        "win_promotion": False, "total_commission": 0, "commission_on_vnd": 0,
    }


def build_message(seq):
    store, store_name, pos = random.choice(STORES)
    staff_id, staff_name = random.choice(STAFF)
    ts = _now_ms()
    items = [_item(p, random.randint(1, 3))
             for p in random.sample(PRODUCTS, random.randint(1, 3))]
    total = sum(it["total_amount"] for it in items)
    before_vat = sum(it["total_amount_without_tax"] for it in items)
    pm = random.choice([0, 0, 0, 1, 4])
    txn_id = f"{store}{pos[-2:]}{staff_id}{seq:04d}"
    sale_txn = {
        "transaction_id": txn_id, "pos_id": pos, "staff_id": staff_id, "staff_name": staff_name,
        "document_date": ts, "posting_date": ts + 200,
        "transaction_record_start_time": ts - 30000, "transaction_record_end_time": ts,
        "payment_method": pm, "payment_method_value": 100,
        "payment_supplier_name": "TIEN MAT" if pm == 0 else "NGAN HANG", "receivable_supplier_id": None,
        "cash": total if pm == 0 else 0, "voucher": None,
        "total_amount": total, "total_amount_without_promotion": total, "promotion_total_amount": 0,
        "amount_before_vat": before_vat, "amount_vat": total - before_vat,
        "customer_code": None, "customer_gender": random.choice([0, 1, 2]),
        "customer_nationality": 0, "customer_age_range": 0,
        "delivery_order_no": None, "delivery_no": None,
        "movement_type": 2, "service_name": "Sale", "transaction_type": 1, "has_invoice": False,
        "transaction_promotions": [], "sale_bom_items": [], "removed_items": [], "user_info": [],
        "sale_normal_items": items,
    }
    return {
        "stream_type_group": "SALE_TRANSACTION", "created_at": ts, "updated_at": ts, "parent_created_at": ts,
        "source": store, "uid": str(uuid.uuid4()), "retry_times": 0, "aud": "LOYALTY_SERVICE;",
        "payload": {"store_code": store, "store_name": store_name, "sale_transaction": [sale_txn]},
        "stream_id": txn_id, "stream_type": "SALE_TRANSACTION", "topic": "store_operation", "status": 0,
    }


def _make_producer():
    from confluent_kafka import Producer
    conf = {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": os.getenv("KAFKA_MECHANISM", "SCRAM-SHA-256"),
        "sasl.username": os.environ["KAFKA_USER"],
        "sasl.password": os.environ["KAFKA_PASS"],
    }
    if os.getenv("KAFKA_CA"):
        conf["ssl.ca.location"] = os.environ["KAFKA_CA"]
    return Producer(conf)


_producer = None
_lock = threading.Lock()


def producer():
    global _producer
    if _producer is None:
        with _lock:
            if _producer is None:
                _producer = _make_producer()
    return _producer


def produce_batch(n):
    p = producer()
    for i in range(n):
        m = build_message(i + 1)
        p.produce(TOPIC, key=m["stream_id"], value=json.dumps(m))
    p.flush(10)
    return n


app = FastAPI(title="store_operation producer", version="1.0.0")


@app.get("/health")
def health():
    return {"status": "ok", "topic": TOPIC}


@app.get("/sample")
def sample():
    return build_message(1)


@app.post("/produce")
def produce_endpoint(n: int = BATCH):
    return {"produced": produce_batch(min(n, 500)), "topic": TOPIC}


def _auto_loop(interval):
    while True:
        try:
            produce_batch(BATCH)
        except Exception as e:  # noqa: BLE001
            print("auto-produce failed:", str(e)[:200])
        time.sleep(interval)


@app.on_event("startup")
def _maybe_stream():
    iv = int(os.getenv("STREAM_INTERVAL_SEC", "0"))
    if iv > 0:
        threading.Thread(target=_auto_loop, args=(iv,), daemon=True).start()
        print(f"auto-producing {BATCH} msgs every {iv}s to {TOPIC}")
