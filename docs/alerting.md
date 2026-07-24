# Alerting — Activator (native) vs GitHub monitor

**Failure drill executed: 2026-07-23.** Deliberately ran the PROD pipeline for
`2025-12-15` (no source data): the DQ gate failed loudly (`row_count > 0 → rows=0`),
`nb_bi_refresh` never ran (dashboards untouched), the monitor workflow detected the
Failed run and **auto-opened GitHub issue #1** (→ email). Chain verified end-to-end.

## Two channels, one failure event

| | GitHub monitor (đang chạy) | Fabric Activator (native) |
|---|---|---|
| Cơ chế | `monitor.yml` cron 01:45 VN → `tools/check_runs.py` quét Job API 26h → mở GitHub issue | Rule trên **Fabric Job events** (Real-Time hub) → action Email/Teams |
| Độ trễ | Tối đa ~24h (cron) — hoặc dispatch tay | **Gần real-time** (event-driven) |
| Setup | Đã chạy, zero-consent (dùng GITHUB_TOKEN) | Cần tạo rule trên UI (consent email action) — API chỉ tạo được vỏ Reflex item, schema rule (ReflexEntities.json) không public |
| Audit trail | Issue = có lịch sử, comment, close | Email/Teams — trôi theo inbox |
| Chi phí | GitHub Actions free tier | Chạy trên capacity Fabric |
| Ngoài-Fabric coverage | Có thể mở rộng quét bất kỳ API nào | Chỉ event trong Fabric |

**Khuyến nghị: chạy CẢ HAI** — Activator cho tốc độ (biết ngay khi đỏ), GitHub issue cho
audit trail (mỗi sự cố một issue, đóng khi xử lý xong). Không trùng lặp vai trò.

## Tạo Activator rule (UI, ~5 phút — một lần)

1. Workspace → **Real-Time** hub → tab **Fabric events** → **Job events** → **Set alert**
2. Filter: `Item = Pipeline_eod_sale_product`, `Job status = Failed`
3. Condition: on each event · Action: **Email**
4. Save vào Activator item (đã có sẵn `act_pipeline_failures` tạo qua API trong DEV,
   hoặc tạo mới trong PROD)

## Ghi chú kỹ thuật từ B3

- `POST /workspaces/{ws}/items {type: "Reflex"}` tạo được item rỗng (201) —
  nhưng definition (`ReflexEntities.json`) không có schema công khai → **rule
  authoring là việc của UI**, đừng cố hand-craft JSON.
- Sản phẩm phụ vô hại của drill: partition rỗng `2025-12-15` trong gold
  (0 dòng — không hiện trên dashboard; lần chạy lại ngày đó sẽ replaceWhere đè).

## eod_sale_service (thêm 2026-07-24)

- `Pipeline_eod_sale_service` (PROD `6b2b5a98-…`) đã thêm vào `tools/check_runs.py`
  WATCH → monitor.yml tự động cảnh báo khi fail, giống 2 pipeline sản phẩm.
- Lịch chạy: đã tạo **Daily 01:00 VN** (`SE Asia Standard Time`) nhưng **DISABLED**
  (`enabled=false`) vì Atlas hiện chỉ có data synthetic cho một khoảng ngày cố định —
  bật lịch khi Mongo có data hằng ngày (hoặc seed synthetic phủ ngày hiện tại), nếu
  không DQ sẽ đỏ mỗi ngày và monitor mở issue rác. Bật: schedule API `enabled=true`
  hoặc toggle trên UI (pipeline → Schedule).
- Activator: rule tương tự có thể thêm cho `Pipeline_eod_sale_service` (Job events →
  Failed) theo đúng các bước ở trên.
