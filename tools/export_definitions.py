"""Export every workspace item definition into fabric_items/ (backup / poor-man's git sync).

Covers: Notebook (ipynb), DataPipeline, SemanticModel (TMDL), Report (PBIR).
Lakehouse/Environment have no exportable definition — the wheel lives in dist/ + CI,
data is rebuildable (simulators + backfill).

Usage:  python tools/export_definitions.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from fabric_api import WS, decode, get_definition, list_items

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "fabric_items")

TYPE_PATHS = {  # itemType -> (api path segment, definition format)
    "Notebook": ("notebooks", "ipynb"),
    "DataPipeline": ("dataPipelines", None),
    "SemanticModel": ("semanticModels", None),
    "Report": ("reports", None),
}


def main() -> None:
    items = list_items(WS)
    # manifest: (old) item ids so deploy_definitions.py can remap references when
    # restoring into a NEW workspace (pipeline->notebook ids, report->model id,
    # DirectLake expression -> workspace/lakehouse ids, notebook -> lakehouse/env ids).
    os.makedirs(OUT, exist_ok=True)
    import json
    with open(os.path.join(OUT, "manifest.json"), "w") as f:
        json.dump({"workspaceId": WS,
                   "items": [{"id": i["id"], "type": i["type"],
                              "displayName": i["displayName"]} for i in items]},
                  f, indent=1, sort_keys=True)
    print(f"manifest: {len(items)} items")
    exported = 0
    for it in sorted(items, key=lambda x: (x["type"], x["displayName"])):
        tp = TYPE_PATHS.get(it["type"])
        if not tp:
            continue
        path_seg, fmt = tp
        safe = it["displayName"].replace("/", "_")
        base = os.path.join(OUT, it["type"], safe)
        try:
            parts = get_definition(path_seg, it["id"], fmt)
        except Exception as e:
            print(f"SKIP {it['type']}/{safe}: {e}")
            continue
        for p in parts:
            dest = os.path.join(base, p["path"])
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with open(dest, "wb") as f:
                f.write(decode(p))
        print(f"exported {it['type']}/{safe} ({len(parts)} parts)")
        exported += 1
    print(f"done: {exported} items -> {OUT}/")


if __name__ == "__main__":
    main()
