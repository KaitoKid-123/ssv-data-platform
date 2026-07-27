// Cloudflare Worker — synthetic `store_operation` producer for the eod_sale_product
// REALTIME (speed) layer. Emits SALE_TRANSACTION events to Aiven Kafka so a Fabric
// Spark Structured Streaming job can build the realtime fact (the Fabric equivalent of
// ClickHouse's rlt_sale_product MV + eod_sale_product_view_rlt).
//
// Message shape mirrors the REAL raw payload in ClickHouse
// (data_kafka.store_operation_mt): a JSON envelope with stream_type="SALE_TRANSACTION"
// and payload.sale_transaction[0].sale_normal_items[*]. VALUES are synthetic (no real
// customer/supplier data) but structurally faithful.
//
// Cloudflare can't speak the native Kafka protocol, so this produces over Aiven's
// **Kafka REST API (Karapace)** — enable it on the Aiven service, then set secrets:
//   wrangler secret put KAFKA_REST_URL   # https://<svc>-<proj>.aivencloud.com:<rest-port>
//   wrangler secret put KAFKA_USER       # e.g. avnadmin
//   wrangler secret put KAFKA_PASS
//   (KAFKA_TOPIC + BATCH set as [vars] in wrangler.toml; default topic "store_operation")
//
// Runs on a cron trigger (see wrangler.toml) AND on demand:
//   GET  /health          -> {status:"ok"}
//   POST /produce?n=20    -> generate n txns, produce, return offsets
//   GET  /sample          -> one generated envelope (inspect the shape, no produce)

const STORES = [
  ["1079", "P4-SH.07 VCP BTH", "107902"],
  ["1031", "Millenium Masteri D4", "103101"],
  ["1164", "246 Bui Vien D1", "116401"],
  ["1078", "S5.01-SH10 VGP THD", "107801"],
];
const STAFF = [
  ["1005675", "Nhan Vien A"], ["1010277", "Nhan Vien B"],
  ["1012047", "Nhan Vien C"], ["1029597", "Nhan Vien D"],
];
// Synthetic catalog shaped like sale_normal_items (fictional products/suppliers).
const PRODUCTS = [
  { code: "96010001", pid: 10001, name: "Coca-Cola 330ml", uom: "CAN", grp_id: 21, grp: "Soft Drinks",
    sub_id: 211, sub: "Carbonated", cat_id: 2000, cat: "Beverage", biz: "Z01 - Normal Merchandise",
    sup: "3900001", sup_name: "SUPPLIER ALPHA", vat: "O3", price: 12000, cost: 8000 },
  { code: "96010002", pid: 10002, name: "Snack Khoai Tay 55g", uom: "BAG", grp_id: 11, grp: "Snacks",
    sub_id: 111, sub: "Chips", cat_id: 1000, cat: "Food", biz: "Z01 - Normal Merchandise",
    sup: "3900002", sup_name: "SUPPLIER BETA", vat: "O3", price: 18000, cost: 12500 },
  { code: "86019244", pid: 19244, name: "Bang Keo OPP 40 Yard", uom: "RO", grp_id: 48, grp: "Stationery",
    sub_id: 984, sub: "Packing and Adhesives", cat_id: 176, cat: "Office Supplies", biz: "Z01 - Normal Merchandise",
    sup: "3911170", sup_name: "SUPPLIER GAMMA", vat: "O3", price: 15000, cost: 9112 },
  { code: "96010004", pid: 10004, name: "Nuoc Suoi 500ml", uom: "BOT", grp_id: 22, grp: "Water",
    sub_id: 221, sub: "Still Water", cat_id: 2000, cat: "Beverage", biz: "Z01 - Normal Merchandise",
    sup: "3900004", sup_name: "SUPPLIER DELTA", vat: "O3", price: 8000, cost: 5200 },
];

const nowMs = () => Date.now();
const pick = (a) => a[Math.floor(Math.random() * a.length)];
const uuid = () => crypto.randomUUID();

function buildItem(p, qty) {
  const total = p.price * qty;
  const noTax = Math.round(total / 1.1);         // 10% VAT
  const costW = Math.round(p.cost * 1.08);
  return {
    product_code: p.code, product_id: p.pid, product_uom_id: 24000 + p.pid % 1000,
    product_name: p.name, uom: p.uom, uom_size: 1, quantity: qty,
    retail_selling_price: p.price,
    product_group_id: p.grp_id, product_group: p.grp,
    product_sub_category_id: p.sub_id, product_sub_category: p.sub,
    product_category_id: p.cat_id, product_category: p.cat,
    retail_business_type: p.biz, supplier_code: p.sup, supplier_name: p.sup_name,
    output_vat_code: p.vat,
    purchase_price_without_tax: p.cost, purchase_price_with_tax: costW,
    total_amount_without_tax: noTax, total_amount: total, sub_total: total,
    win_promotion: false, total_commission: 0, commission_on_vnd: 0,
  };
}

// One synthetic SALE_TRANSACTION envelope (same shape as store_operation_mt.payload).
function buildMessage(seq) {
  const [store, storeName, pos] = pick(STORES);
  const [staffId, staffName] = pick(STAFF);
  const ts = nowMs();
  const n = 1 + Math.floor(Math.random() * 3);
  const chosen = [...PRODUCTS].sort(() => Math.random() - 0.5).slice(0, n);
  const items = chosen.map((p) => buildItem(p, 1 + Math.floor(Math.random() * 3)));
  const total = items.reduce((s, it) => s + it.total_amount, 0);
  const beforeVat = items.reduce((s, it) => s + it.total_amount_without_tax, 0);
  const pm = pick([0, 0, 0, 1, 4]);              // 0 cash mostly, 1 card, 4 e-wallet
  const txnId = `${store}${pos.slice(-2)}${staffId}${String(seq).padStart(4, "0")}`;

  const saleTxn = {
    transaction_id: txnId, pos_id: pos, staff_id: staffId, staff_name: staffName,
    document_date: ts, posting_date: ts + 200,
    transaction_record_start_time: ts - 30000, transaction_record_end_time: ts,
    payment_method: pm, payment_method_value: 100,
    payment_supplier_name: pm === 0 ? "TIEN MAT" : "NGAN HANG", receivable_supplier_id: null,
    cash: pm === 0 ? total : 0, voucher: null,
    total_amount: total, total_amount_without_promotion: total, promotion_total_amount: 0,
    amount_before_vat: beforeVat, amount_vat: total - beforeVat,
    customer_code: null, customer_gender: pick([0, 1, 2]), customer_nationality: 0, customer_age_range: 0,
    delivery_order_no: null, delivery_no: null,
    movement_type: 2, service_name: "Sale", transaction_type: 1, has_invoice: false,
    transaction_promotions: [], sale_bom_items: [], removed_items: [], user_info: [],
    sale_normal_items: items,
  };
  return {
    stream_type_group: "SALE_TRANSACTION", created_at: ts, updated_at: ts, parent_created_at: ts,
    source: store, uid: uuid(), retry_times: 0, aud: "LOYALTY_SERVICE;",
    payload: { store_code: store, store_name: storeName, sale_transaction: [saleTxn] },
    stream_id: txnId, stream_type: "SALE_TRANSACTION", topic: "store_operation", status: 0,
  };
}

async function produce(env, messages) {
  const topic = env.KAFKA_TOPIC || "store_operation";
  const url = `${env.KAFKA_REST_URL.replace(/\/$/, "")}/topics/${topic}`;
  const body = JSON.stringify({
    // key by transaction_id so all lines of a txn land on one partition, in order
    records: messages.map((m) => ({ key: m.stream_id, value: m })),
  });
  const auth = "Basic " + btoa(`${env.KAFKA_USER}:${env.KAFKA_PASS}`);
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/vnd.kafka.json.v2+json", Accept: "application/vnd.kafka.v2+json", Authorization: auth },
    body,
  });
  const text = await resp.text();
  if (!resp.ok) throw new Error(`Kafka REST ${resp.status}: ${text.slice(0, 300)}`);
  return JSON.parse(text);
}

function batch(env, n) {
  const size = n || parseInt(env.BATCH || "10", 10);
  return Array.from({ length: size }, (_, i) => buildMessage(i + 1));
}

export default {
  // Cron trigger: produce a batch each schedule tick (see wrangler.toml [triggers]).
  async scheduled(event, env, ctx) {
    ctx.waitUntil(produce(env, batch(env)).catch((e) => console.error("produce failed:", e.message)));
  },
  async fetch(request, env) {
    const { pathname, searchParams } = new URL(request.url);
    if (pathname === "/health") return Response.json({ status: "ok" });
    if (pathname === "/sample") return Response.json(buildMessage(1));   // inspect shape, no produce
    if (pathname === "/produce" && request.method === "POST") {
      const n = Math.min(parseInt(searchParams.get("n") || env.BATCH || "10", 10), 500);
      try {
        const res = await produce(env, batch(env, n));
        return Response.json({ produced: n, offsets: res.offsets });
      } catch (e) {
        return new Response(e.message, { status: 502 });
      }
    }
    return new Response("not found", { status: 404 });
  },
};
