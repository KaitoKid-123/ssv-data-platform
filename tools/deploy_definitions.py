"""Restore / deploy / promote workspace items from fabric_items/ (the export backup).

Layout expected: fabric_items/<displayName>.<Type>/ with a .platform descriptor per
item (git-integration format — shared with fabric-cicd and native Git integration).

Modes:
  Same workspace (default)      -> updateDefinition in place (push edits / undo UI mistakes)
  Other workspace (--workspace) -> PROMOTION or DISASTER RECOVERY: creates missing items
                                   and REMAPS every GUID reference (ws/lakehouse/notebook/
                                   model ids) using fabric_items/manifest.json + matching
                                   items in the target by displayName.

Items publish in dependency order BETWEEN types (Notebook -> SemanticModel -> Report ->
DataPipeline) and in PARALLEL within each type (lesson borrowed from fabric-cicd).

Usage:
  python tools/deploy_definitions.py --dry-run
  python tools/deploy_definitions.py --item nb_bi_refresh.py
  python tools/deploy_definitions.py --only Notebook
  python tools/deploy_definitions.py --workspace <ws-guid>       # promote / DR
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
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(__file__))
from fabric_api import WS, call, list_items

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "fabric_items")

TYPE_PATHS = {"Notebook": ("notebooks", "ipynb"), "SemanticModel": ("semanticModels", None),
              "Report": ("reports", None), "DataPipeline": ("dataPipelines", None)}
ORDER = ["Notebook", "SemanticModel", "Report", "DataPipeline"]
INFRA = {"Lakehouse": "lakehouses", "Environment": "environments"}
PARALLELISM = 6


def load_manifest() -> dict:
    with open(os.path.join(SRC, "manifest.json")) as f:
        return json.load(f)


def discover() -> dict:
    """{itemType: [(displayName, dirpath)]} — identity read from each .platform file."""
    found: dict = {t: [] for t in ORDER}
    for name in sorted(os.listdir(SRC)):
        d = os.path.join(SRC, name)
        plat = os.path.join(d, ".platform")
        if not (os.path.isdir(d) and os.path.isfile(plat)):
            continue
        with open(plat) as f:
            meta = json.load(f)["metadata"]
        if meta["type"] in found:
            found[meta["type"]].append((meta["displayName"], d))
    return found


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


def deploy_one(ws: str, itype: str, name: str, item_dir: str,
               existing: dict, remap: dict) -> tuple[str, str, str]:
    """Publish one item (thread-safe: reads remap, returns ids for the main thread)."""
    seg, fmt = TYPE_PATHS[itype]
    definition = {"parts": parts_from_dir(item_dir, remap)}
    if fmt:
        definition["format"] = fmt
    if (itype, name) in existing:
        tid = existing[(itype, name)]
        call("POST", f"/workspaces/{ws}/{seg}/{tid}/updateDefinition",
             json={"definition": definition})
        return name, tid, "updated"
    r = call("POST", f"/workspaces/{ws}/{seg}",
             json={"displayName": name, "definition": definition}, expect_lro_result=True)
    return name, r.json()["id"], "created"


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

    found = discover()
    done = 0
    for itype in ORDER:                                 # dependency order between types
        batch = [(n, d) for n, d in found.get(itype, [])
                 if not (args.only and itype != args.only)
                 and not (args.item and n != args.item)]
        if not batch:
            continue
        if args.dry_run:
            for name, _ in batch:
                verb = "UPDATE" if (itype, name) in existing else "CREATE"
                print(f"[dry] would {verb} {itype}/{name}")
            done += len(batch)
            continue
        with ThreadPoolExecutor(max_workers=PARALLELISM) as pool:   # parallel within type
            futures = [pool.submit(deploy_one, ws, itype, n, d, existing, remap)
                       for n, d in batch]
            for fut in futures:
                name, tid, action = fut.result()
                print(f"{action} {itype}/{name}" + (f" -> {tid}" if action == "created" else ""))
                existing[(itype, name)] = tid
                old_id = manifest_id(man, itype, name)
                if old_id and tid != old_id:
                    remap[old_id] = tid          # later types reference this one
                done += 1
    print(f"done: {done} item(s){' (dry-run)' if args.dry_run else ''}")
    if ws != man["workspaceId"] and not args.dry_run and not (args.item or args.only):
        print("\nDR checklist còn lại: tools/deploy_wheel.py (wheel vào Environment mới),"
              "\n  seed data (simulators) + backfill, gắn lại Mongo connection + schedule.")


if __name__ == "__main__":
    main()
