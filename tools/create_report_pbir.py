"""Create report 'EOD Sales - Product & Customer' (PBIR enhanced, 2 pages) via API.

Layout mirrors docs/superpowers/specs/2026-07-15-dashboard-extension-design.md.
Binds to the same semantic model as the existing dashboard; existing report untouched.
"""
import base64
import json
import sys
import uuid

sys.path.insert(0, "/tmp/claude-1000/-home-khang-Fabric-Platform-ssv-data-platform/bd3837a8-c86f-419a-a764-0dfd57d26481/scratchpad")
from fabric_api import call, WS

D = "/tmp/claude-1000/-home-khang-Fabric-Platform-ssv-data-platform/bd3837a8-c86f-419a-a764-0dfd57d26481/scratchpad/report/"
VC_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.10.0/schema.json"
PAGE_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.1.0/schema.json"
PAGES_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.1.0/schema.json"

nid = lambda: uuid.uuid4().hex[:20]


def col(entity, prop):
    return {"Column": {"Expression": {"SourceRef": {"Entity": entity}}, "Property": prop}}


def mea(entity, prop):
    return {"Measure": {"Expression": {"SourceRef": {"Entity": entity}}, "Property": prop}}


def proj(field, active=False):
    kind = list(field.keys())[0]
    ent = field[kind]["Expression"]["SourceRef"]["Entity"]
    prop = field[kind]["Property"]
    p = {"field": field, "queryRef": f"{ent}.{prop}", "nativeQueryRef": prop}
    if active:
        p["active"] = True
    return p


def title(text):
    return {"title": [{"properties": {"text": {"expr": {"Literal": {"Value": f"'{text}'"}}}}}]}


def visual(vtype, pos, roles, sort=None, vc_objects=None, objects=None, extra=None):
    x, y, w, h, z = pos
    v = {"visualType": vtype,
         "query": {"queryState": {r: {"projections": ps} for r, ps in roles.items()}},
         "drillFilterOtherVisuals": True}
    if sort:
        v["query"]["sortDefinition"] = {"sort": [sort], "isDefaultSort": True}
    if vc_objects:
        v["visualContainerObjects"] = vc_objects
    if objects:
        v["objects"] = objects
    if extra:
        v.update(extra)
    return {"$schema": VC_SCHEMA, "name": nid(),
            "position": {"x": x, "y": y, "z": z, "height": h, "width": w, "tabOrder": z},
            "visual": v}


def slicer(pos, field, mode, sync_name, header):
    return visual("slicer", pos, {"Values": [proj(field, active=True)]},
                  objects={"data": [{"properties": {"mode": {"expr": {"Literal": {"Value": f"'{mode}'"}}}}}]},
                  vc_objects=title(header),
                  extra={"syncGroup": {"groupName": sync_name, "fieldChanges": True, "filterChanges": True}})


def card(pos, measure_field):
    p = proj(measure_field)
    p["format"] = "G"
    return visual("cardVisual", pos, {"Data": [p]})


S = "Sales"
P = "dim_product"
DT = "dim_date"
desc = lambda f: {"field": f, "direction": "Descending"}
asc = lambda f: {"field": f, "direction": "Ascending"}

# ---------------- Page 1: Product & Margin ----------------
pg1 = [
    slicer((0, 0, 300, 68, 0), col(DT, "date"), "Between", "date", "Date"),
    slicer((310, 0, 250, 68, 1), col(S, "channel"), "Basic", "channel", "Channel"),
    card((0, 78, 310, 110, 2), mea(S, "Revenue")),
    card((330, 78, 310, 110, 3), mea(S, "Gross Margin")),
    card((660, 78, 310, 110, 4), mea(S, "Margin %")),
    visual("clusteredBarChart", (0, 198, 630, 250, 5),
           {"Category": [proj(col(P, "product_name"), active=True)],
            "Y": [proj(mea(S, "Revenue"))],
            "Tooltips": [proj(mea(S, "Margin %"))]},
           sort=desc(mea(S, "Revenue")), vc_objects=title("Top Products by Revenue")),
    visual("treemap", (640, 198, 640, 250, 6),
           {"Group": [proj(col(P, "product_category"), active=True)],
            "Details": [proj(col(P, "product_group"))],
            "Values": [proj(mea(S, "Revenue"))]},
           vc_objects=title("Revenue by Category / Group")),
    visual("pivotTable", (0, 458, 630, 250, 7),
           {"Rows": [proj(col(P, "product_category"), active=True)],
            "Columns": [proj(col(S, "channel"))],
            "Values": [proj(mea(S, "Revenue")), proj(mea(S, "Margin %"))]},
           vc_objects=title("Category x Channel")),
    visual("scatterChart", (640, 458, 640, 250, 8),
           {"Category": [proj(col(P, "product_name"), active=True)],
            "X": [proj(mea(S, "Revenue"))],
            "Y": [proj(mea(S, "Margin %"))],
            "Size": [proj(mea(S, "Units"))]},
           vc_objects=title("Margin % vs Revenue (size = Units)")),
]

# ---------------- Page 2: Customer ----------------
pg2 = [
    slicer((0, 0, 300, 68, 0), col(DT, "date"), "Between", "date", "Date"),
    slicer((310, 0, 250, 68, 1), col(S, "channel"), "Basic", "channel", "Channel"),
    card((0, 78, 310, 110, 2), mea(S, "Unique Customers")),
    card((330, 78, 310, 110, 3), mea(S, "Identified Rate %")),
    card((660, 78, 310, 110, 4), mea(S, "Repeat Rate %")),
    card((990, 78, 290, 110, 5), mea(S, "Avg Rating")),
    visual("donutChart", (0, 198, 420, 250, 6),
           {"Category": [proj(col(S, "customer_gender"), active=True)],
            "Y": [proj(mea(S, "Baskets"))]},
           sort=desc(mea(S, "Baskets")), vc_objects=title("Baskets by Gender")),
    visual("clusteredBarChart", (430, 198, 420, 250, 7),
           {"Category": [proj(col(S, "customer_age_range"), active=True)],
            "Y": [proj(mea(S, "Baskets"))]},
           sort=desc(mea(S, "Baskets")), vc_objects=title("Baskets by Age Range")),
    visual("clusteredBarChart", (860, 198, 420, 250, 8),
           {"Category": [proj(col(S, "channel"), active=True)],
            "Y": [proj(mea(S, "Avg Rating"))]},
           sort=desc(mea(S, "Avg Rating")),
           vc_objects=title("Avg Rating by Channel (delivery only)")),
    visual("lineChart", (0, 458, 1280, 250, 9),
           {"Category": [proj(col(DT, "date"), active=True)],
            "Y": [proj(mea(S, "Identified Rate %"))]},
           sort=asc(col(DT, "date")), vc_objects=title("Identified Rate % by Day")),
]

# ---------------- assemble parts ----------------
parts = {}
# reuse binding, report settings and themes from the existing report verbatim
for reuse in ["definition.pbir", "definition__version.json", "definition__report.json",
              "definition__StaticResources__SharedResources__BaseThemes__CY26SU07.json",
              "definition__StaticResources__SharedResources__BuiltInThemes__NewExecutive.json"]:
    try:
        parts[reuse.replace("__", "/").replace("definition/StaticResources", "StaticResources")] = \
            open(D + reuse, "rb").read()
    except FileNotFoundError:
        print("skip missing", reuse)

# fix path names for static resources (stored with definition__ prefix locally? handle both)
import os
for fn in os.listdir(D):
    if fn.startswith("StaticResources__"):
        parts[fn.replace("__", "/")] = open(D + fn, "rb").read()

page_ids = []
for disp, visuals in [("Product & Margin", pg1), ("Customer", pg2)]:
    pid = nid()
    page_ids.append(pid)
    parts[f"definition/pages/{pid}/page.json"] = json.dumps({
        "$schema": PAGE_SCHEMA, "name": pid, "displayName": disp,
        "displayOption": "FitToPage", "height": 720, "width": 1280}).encode()
    for vc in visuals:
        parts[f"definition/pages/{pid}/visuals/{vc['name']}/visual.json"] = \
            json.dumps(vc, ensure_ascii=False).encode()

parts["definition/pages/pages.json"] = json.dumps({
    "$schema": PAGES_SCHEMA, "pageOrder": page_ids, "activePageName": page_ids[0]}).encode()

payload = [{"path": p, "payload": base64.b64encode(b).decode(), "payloadType": "InlineBase64"}
           for p, b in parts.items()]
print("parts:", sorted(parts))
r = call("POST", f"/workspaces/{WS}/reports",
         json={"displayName": "EOD Sales - Product & Customer",
               "description": "Auto-generated: product/margin + customer analytics on model Sales",
               "definition": {"parts": payload}})
print("create:", r.status_code)
print(r.text[:400])
