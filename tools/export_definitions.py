"""Export every workspace item definition into fabric_items/ (backup / promotion / DR source).

Layout: fabric_items/<displayName>.<Type>/  — Fabric git-integration format, so the
same folder works for tools/deploy_definitions.py, fabric-cicd AND native Git
integration (see docs/fabric-cicd-evaluation.md).

Covers: Notebook (ipynb), DataPipeline, SemanticModel (TMDL), Report (PBIR).
Lakehouse/Environment have no exportable definition — the wheel lives in dist/ + CI,
data is rebuildable (simulators + backfill).

Usage:  python tools/export_definitions.py [--workspace WS_ID]
        (default: PROD. Pass the DEV workspace id to capture DEV work into
         fabric_items/ before promoting it with deploy_definitions.py.)
"""
import json
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(__file__))
from fabric_api import WS, decode, get_definition, list_items

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "fabric_items")
KEEP_FILES = {"manifest.json", "parameter.yml"}   # root-level metadata, never pruned

TYPE_PATHS = {  # itemType -> (api path segment, definition format)
    "Notebook": ("notebooks", "ipynb"),
    "DataPipeline": ("dataPipelines", None),
    "SemanticModel": ("semanticModels", None),
    "Report": ("reports", None),
}


def main() -> None:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--workspace", default=WS, help="workspace to export (default: PROD)")
    ws = ap.parse_args().workspace
    items = list_items(ws)
    # manifest: (old) item ids so deploy_definitions.py can remap references when
    # restoring into a NEW workspace (pipeline->notebook ids, report->model id,
    # DirectLake expression -> workspace/lakehouse ids, notebook -> lakehouse/env ids).
    os.makedirs(OUT, exist_ok=True)
    with open(os.path.join(OUT, "manifest.json"), "w") as f:
        json.dump({"workspaceId": ws,
                   "items": [{"id": i["id"], "type": i["type"],
                              "displayName": i["displayName"]} for i in items]},
                  f, indent=1, sort_keys=True)
    print(f"manifest: {len(items)} items")
    exported = 0
    current: set = set()
    for it in sorted(items, key=lambda x: (x["type"], x["displayName"])):
        tp = TYPE_PATHS.get(it["type"])
        if not tp:
            continue
        path_seg, fmt = tp
        safe = it["displayName"].replace("/", "_")
        folder = f"{safe}.{it['type']}"
        base = os.path.join(OUT, folder)
        try:
            parts = get_definition(path_seg, it["id"], fmt, ws=ws)
        except Exception as e:
            print(f"SKIP {folder}: {e}")
            continue
        for p in parts:
            dest = os.path.join(base, p["path"])
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with open(dest, "wb") as f:
                f.write(decode(p))
        print(f"exported {folder} ({len(parts)} parts)")
        exported += 1
        current.add(folder)
    # prune anything that is no longer in the workspace (or legacy-layout leftovers)
    for name in os.listdir(OUT):
        full = os.path.join(OUT, name)
        if os.path.isfile(full):
            if name not in KEEP_FILES:
                os.remove(full)
                print(f"pruned stale file {name}")
        elif name not in current:
            shutil.rmtree(full)
            print(f"pruned stale {name}")
    print(f"done: {exported} items -> {OUT}/")


if __name__ == "__main__":
    main()
