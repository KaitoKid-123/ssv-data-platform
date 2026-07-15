"""Restore / deploy workspace items from fabric_items/ (the export_definitions backup).

Two modes:
  Same workspace (default)      -> updateDefinition in place (push local edits / undo UI mistakes)
  New workspace (--workspace X) -> DISASTER RECOVERY: creates missing items and REMAPS all
                                   GUID references (old ws/lakehouse/notebook/model ids -> new)
                                   using fabric_items/manifest.json written at export time.

Deploy order respects references: infra (Lakehouse/Environment, created empty if missing)
-> Notebook -> SemanticModel -> Report (needs model id) -> DataPipeline (needs notebook ids).

Usage:
  python tools/deploy_definitions.py --dry-run
  python tools/deploy_definitions.py --item nb_bi_refresh.py
  python tools/deploy_definitions.py --only Notebook
  python tools/deploy_definitions.py --workspace <new-ws-guid>       # DR
Notes:
  - Lakehouse DATA is not restored (rebuild: simulators seed + pipeline backfill).
  - Environment wheel is not restored here (run tools/deploy_wheel.py after).
Auth: SPN env vars or az CLI login (see fabric_api.token()).
"""
import argparse
import base64
import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from fabric_api import WS, call, list_items

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "fabric_items")

TYPE_PATHS = {"Notebook": ("notebooks", "ipynb"), "SemanticModel": ("semanticModels", None),
              "Report": ("reports", None), "DataPipeline": ("dataPipelines", None)}
ORDER = ["Notebook", "SemanticModel", "Report", "DataPipeline"]
INFRA = {"Lakehouse": "lakehouses", "Environment": "environments"}


def load_manifest() -> dict:
    with open(os.path.join(SRC, "manifest.json")) as f:
        return json.load(f)


def manifest_id(man: dict, itype: str, name: str) -> str | None:
    for it in man["items"]:
        if it["type"] == itype and it["displayName"] == name:
            return it["id"]
    return None


def parts_from_dir(item_dir: str, remap: dict) -> list[dict]:
    parts = []
    for dirpath, _, files in os.walk(item_dir):
        for fn in files:
            full = os.path.join(dirpath, fn)
            rel = os.path.relpath(full, item_dir).replace(os.sep, "/")
            with open(full, "rb") as f:
                raw = f.read()
            for old, new in remap.items():
                raw = raw.replace(old.encode(), new.encode())
            parts.append({"path": rel, "payload": base64.b64encode(raw).decode(),
                          "payloadType": "InlineBase64"})
    return parts


def ensure_infra(ws: str, man: dict, existing: dict, remap: dict, dry: bool) -> None:
    """Lakehouse/Environment can't be restored from a definition — create empty if missing,
    and remap their old ids so notebook metadata / DirectLake expressions stay valid."""
    for it in man["items"]:
        seg = INFRA.get(it["type"])
        if not seg:
            continue
        key = (it["type"], it["displayName"])
        if key in existing:
            new_id = existing[key]
        elif dry:
            print(f"[dry] would CREATE {it['type']} '{it['displayName']}'")
            continue
        else:
            body = {"displayName": it["displayName"]}
            if it["type"] == "Lakehouse":
                body["creationPayload"] = {"enableSchemas": True}   # bronze/silver/gold schemas
            r = call("POST", f"/workspaces/{ws}/{seg}", json=body, expect_lro_result=True)
            new_id = r.json()["id"]
            existing[key] = new_id
            print(f"created {it['type']} '{it['displayName']}' -> {new_id}")
        if new_id != it["id"]:
            remap[it["id"]] = new_id


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workspace", default=WS, help="target workspace id (default: source ws)")
    ap.add_argument("--only", choices=ORDER, help="deploy only this item type")
    ap.add_argument("--item", help="deploy only the item with this displayName")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    man = load_manifest()
    ws = args.workspace
    existing = {(i["type"], i["displayName"]): i["id"] for i in list_items(ws)}
    remap: dict = {}
    if ws != man["workspaceId"]:
        remap[man["workspaceId"]] = ws
    ensure_infra(ws, man, existing, remap, args.dry_run)

    done = skipped = 0
    for itype in ORDER:
        if args.only and itype != args.only:
            continue
        tdir = os.path.join(SRC, itype)
        if not os.path.isdir(tdir):
            continue
        seg, fmt = TYPE_PATHS[itype]
        for name in sorted(os.listdir(tdir)):
            if args.item and name != args.item:
                continue
            parts = parts_from_dir(os.path.join(tdir, name), remap)
            definition = {"parts": parts}
            if fmt:
                definition["format"] = fmt
            old_id = manifest_id(man, itype, name)
            if (itype, name) in existing:
                tid = existing[(itype, name)]
                if args.dry_run:
                    print(f"[dry] would UPDATE {itype}/{name} ({len(parts)} parts)")
                else:
                    call("POST", f"/workspaces/{ws}/{seg}/{tid}/updateDefinition",
                         json={"definition": definition})
                    print(f"updated {itype}/{name}")
            else:
                if args.dry_run:
                    print(f"[dry] would CREATE {itype}/{name} ({len(parts)} parts)")
                    continue
                r = call("POST", f"/workspaces/{ws}/{seg}",
                         json={"displayName": name, "definition": definition},
                         expect_lro_result=True)
                tid = r.json()["id"]
                existing[(itype, name)] = tid
                print(f"created {itype}/{name} -> {tid}")
            if old_id and tid != old_id:
                remap[old_id] = tid      # later items (report/pipeline) reference this one
            done += 1
    print(f"done: {done} item(s){' (dry-run)' if args.dry_run else ''}")
    if ws != man["workspaceId"] and not args.dry_run:
        print("\nDR checklist còn lại: tools/deploy_wheel.py (wheel vào Environment mới),"
              "\n  attach Environment vào notebooks nếu cần, seed data (simulators) + backfill,"
              "\n  gắn lại schedule/pipeline connections (Mongo connection là connection-level, không nằm trong definition).")


if __name__ == "__main__":
    main()
