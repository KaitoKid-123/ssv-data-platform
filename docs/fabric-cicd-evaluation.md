# fabric-cicd vs tools/ tự viết — đánh giá thực nghiệm

**Ngày thử: 2026-07-16 · fabric-cicd 1.2.0 · kịch bản:** publish 13 notebooks từ
`fabric_items/` (mirror PROD) vào workspace **DEV**, GUID remap qua `parameter.yml`
dynamic variables. **Kết quả: PASS** — 13/13 published song song trong ~6 giây,
attachment lakehouse/environment resolve đúng về GUID DEV (verify bằng API).

## Setup đã dùng

```
scratchpad/fabric_cicd_repo/
  bronze.py.Notebook/          ← convert từ fabric_items/Notebook/bronze.py/
    notebook-content.ipynb       (fabric-cicd cần layout <name>.<Type> — format
    .platform                     git-integration; .platform ta export sẵn dùng được luôn)
  ...
  parameter.yml                ← điểm hay nhất: KHÔNG hardcode GUID đích
```

```yaml
find_replace:
    - find_value: "<PROD lakehouse guid>"
      replace_value:
          DEV: "$items.Lakehouse.lh_eod_sales.$id"   # resolve theo TÊN lúc deploy
    - find_value: "<PROD ws guid>"
      replace_value:
          DEV: "$workspace.$id"
```

```python
fw = FabricWorkspace(repository_directory=..., token_credential=AzureCliCredential(),
                     item_type_in_scope=["Notebook"], environment="DEV", workspace_id=DEV)
publish_all_items(fw)
```

## So găng

| Tiêu chí | fabric-cicd 1.2.0 | tools/ tự viết |
|---|---|---|
| Tốc độ publish | ✅ **Song song** (13 notebooks/6s) | Tuần tự LRO (~2-3 phút) |
| GUID remap | `parameter.yml` + dynamic vars — khai báo, nhưng **mỗi GUID nguồn phải liệt kê tay** | ✅ **Tự động 100%** qua `manifest.json` match theo tên — 0 config |
| Chiều EXPORT (capture/backup) | ❌ Không có — mặc định dựa vào Git integration | ✅ `export_definitions` (nền của backup + promotion + DR) |
| Tạo hạ tầng khi DR (Lakehouse schema-enabled, Environment) | Chưa kiểm chứng | ✅ Đã diễn tập PASS |
| Layout repo | `<name>.<Type>` (chuẩn git-integration của Fabric) | `<Type>/<name>` (tự đặt) |
| Dọn item mồ côi | ✅ `unpublish_all_orphan_items` | Chỉ prune phía git mirror |
| Hỗ trợ dài hạn | ✅ Microsoft maintain, thêm item type liên tục | Tự vá khi API đổi |
| Deploy Environment + libraries | Có hỗ trợ item Environment (chưa thử với wheel) | `deploy_wheel.py` đã chạy prod |

## Kết luận & khuyến nghị

1. **Giữ nguyên bộ tools hiện tại cho vòng đời hằng ngày** (backup, promotion, DR) —
   chiều export và remap-theo-tên tự động là thứ fabric-cicd không có, và cả hai đã
   được kiểm chứng bằng drill thật.
2. **Điểm đáng học từ fabric-cicd**: publish song song (tools tuần tự — có thể cải
   thiện nếu thấy chậm) và layout `<name>.<Type>`.
3. **Lộ trình hợp lý khi dự án lớn lên**: đổi `fabric_items/` sang layout
   `<name>.<Type>` (một converter nhỏ) → mở khóa đồng thời fabric-cicd **và** Fabric
   Git integration native; khi đó CI full-workspace-deploy nên chuyển sang fabric-cicd
   (chính chủ, song song, ít code phải nuôi), tools/ rút về vai trò export + DR.
4. Converter thử nghiệm + parameter.yml mẫu nằm trong scratchpad phiên này; tái tạo
   được trong ~10 phút theo tài liệu này khi cần.
