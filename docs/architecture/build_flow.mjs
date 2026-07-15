// EOD Sales platform — 2 pages (Data Flow + Orchestration/Deploy) built with drawio-ai-kit.
// Run: node build_flow.mjs
import { writeFileSync } from "node:fs";
import { Diagram } from "/home/khang/.npm-global/lib/node_modules/drawio-ai-kit/src/builder.mjs";
import { frame, icon, box, phantom, renderTree } from "/home/khang/.npm-global/lib/node_modules/drawio-ai-kit/src/layout-engine.mjs";

const BRZ = "#d79b00", SLV = "#666666", GLD = "#d6b656", BIC = "#82b366", SRC = "#6c8ebf", PIP = "#9673a6", RED = "#b85450";

// ---------------- Page 1: Data flow (medallion) ----------------
const d1 = new Diagram("network");

const sources = frame("sources", "Sources", { dir: "col", gap: 24, stroke: SRC }, [
  icon("mongo", "mongodb", "MongoDB Atlas"),
  icon("pg", "azure_azure_database_postgresql_server", "PostgreSQL (Aiven)"),
  icon("dlm", "azure_api_management_services", "DLM REST API"),
]);

const bronze = frame("bronze", "BRONZE — staging per run (overwrite)", { dir: "col", gap: 10, stroke: BRZ }, [
  box("b_bills", "sale_bill / sale_return_bill\n(nested JSON string)", { w: 210, h: 46 }),
  box("b_ps", "partner_store (full)", { w: 210, h: 30 }),
  box("b_win", "delivery_orders, sale_transactions,\npoint_histories (windowed)", { w: 210, h: 46 }),
  box("b_child", "order audits + details\n(semi-join on windowed orders)", { w: 210, h: 46 }),
  box("b_full", "users, oms_*, mapping (full)", { w: 210, h: 30 }),
  box("b_dims", "3 dims (full)", { w: 210, h: 30 }),
  box("b_dlm", "partner_sources", { w: 210, h: 30 }),
]);

const silver = frame("silver", "SILVER — conformed", { dir: "col", gap: 10, stroke: SLV }, [
  box("s_line", "sale_line — +7h VN day,\nexplode items, partition report_date\n+ replaceWhere", { w: 220, h: 60 }),
  box("s_promo", "promotion", { w: 220, h: 30 }),
  box("s_enrich", "delivery / transaction /\npoint_history", { w: 220, h: 46 }),
  box("s_scd", "dim_product + dim_store (SCD2)\nvalid_from / valid_to / is_current", { w: 220, h: 46 }),
  box("s_dims", "dim_customer, dim_oms_product,\ndim_partner_store, dim_payment,\ndim_purchase_price", { w: 220, h: 60 }),
]);

const gold = frame("gold", "GOLD — business", { dir: "col", gap: 10, stroke: GLD }, [
  box("g_fact", "fact_eod_sale_product\n~80 cols, grain = sale line\npartition report_date + replaceWhere", { w: 230, h: 60 }),
  box("g_gate", "DQ gate — fail loud", { w: 230, h: 34, fill: "#f8cecc", stroke: RED }),
  box("g_vw", "vw_eod_sales / vw_eod_sales_daily", { w: 230, h: 30 }),
  box("g_bi", "bi_eod_sales / bi_eod_sales_daily\n(CTAS marts)", { w: 230, h: 46 }),
  box("g_dimp", "bi_dim_product (is_current slice)", { w: 230, h: 30 }),
  box("g_date", "dim_date (+ day_type)", { w: 230, h: 30 }),
]);

const lakehouse = frame("lakehouse", "Lakehouse lh_eod_sales (OneLake, schema-enabled)", { dir: "row", gap: 28, align: "top", stroke: "#999999", cornerIcon: "azure_data_lake_storage_gen1" }, [
  bronze, silver, gold,
]);

const bi = frame("bi", "BI — Direct Lake", { dir: "col", gap: 20, stroke: BIC }, [
  icon("model", "power_bi", "Semantic model 'Sales'"),
  box("model_d", "Sales · Sales Daily · dim_date · dim_product\n22 measures (Revenue, Margin %, Repeat Rate %...)", { w: 240, h: 46 }),
  box("report", "Report 'EOD Sales Product Dashboard'\n6 pages, synced slicer strip", { w: 230, h: 46 }),
]);

const fabric = frame("fabric", "Microsoft Fabric — workspace RetailSales_Analysis", { dir: "row", gap: 40, align: "top", stroke: "#0078D4" }, [
  lakehouse, bi,
]);

renderTree(d1, phantom("root1", "EOD Sales — data flow (medallion)", { dir: "row", gap: 50, align: "top" }, [sources, fabric]), [40, 60]);

d1.link("mongo", "b_bills", "Copy windowed");
d1.link("mongo", "b_ps", "Copy full");
d1.link("pg", "b_win", "JDBC windowed");
d1.link("pg", "b_child", "semi-join");
d1.link("pg", "b_full", "JDBC full");
d1.link("pg", "b_dims", "JDBC full");
d1.link("dlm", "b_dlm", "requests");
d1.link("b_bills", "s_line", "");
d1.link("b_bills", "s_promo", "");
d1.link("b_win", "s_enrich", "");
d1.link("b_child", "s_enrich", "");
d1.link("b_dims", "s_scd", "scd2_apply (forward-only)");
d1.link("b_full", "s_dims", "");
d1.link("s_line", "g_fact", "");
d1.link("s_scd", "g_fact", "as_of_join(transaction_time)");
d1.link("s_enrich", "g_fact", "");
d1.link("g_fact", "g_gate", "");
d1.link("g_gate", "g_vw", "pass");
d1.link("g_vw", "g_bi", "CTAS");
d1.link("s_scd", "g_dimp", "WHERE is_current");
d1.link("g_bi", "model", "Direct Lake");
d1.link("g_dimp", "model", "Direct Lake");
d1.link("g_date", "model", "Direct Lake");
d1.link("model", "report", "");

const r1 = d1.validate();
console.log("P1 VALIDATE:", JSON.stringify({ ok: r1.ok, errors: r1.errors, warnings: r1.warnings }));

// ---------------- Page 2: Orchestration + deploy ----------------
const d2 = new Diagram("network");

const ingest = frame("ingest", "parallel ingest", { dir: "col", gap: 8, stroke: PIP }, [
  box("cp1", "cp_mongo_sale_bill (Copy, windowed)", { w: 230, h: 30 }),
  box("cp2", "cp_mongo_return (Copy, windowed)", { w: 230, h: 30 }),
  box("cp3", "cp_mongo_partner_store (Copy, full)", { w: 230, h: 30 }),
  box("nbpg", "nb_ingest_pg (JDBC windowed 0.1.4)", { w: 230, h: 30 }),
  box("nbdlm", "nb_ingest_dlm (requests)", { w: 230, h: 30 }),
]);

const pipeline = frame("pipeline", "Pipeline_eod_sale_product (daily; run_date empty = yesterday VN)", { dir: "row", gap: 52, align: "middle", stroke: PIP, cornerIcon: "azure_data_factories" }, [
  box("setv", "Set variable\nv_run_date", { w: 120, h: 46 }),
  ingest,
  box("tr", "nb_transform (main.py)\nEodSalePipeline.run()\nsilver + gold", { w: 180, h: 60 }),
  box("dq", "nb_dq_check\nfail loud", { w: 120, h: 46, fill: "#f8cecc", stroke: RED }),
  box("birf", "nb_bi_refresh\nviews + CTAS marts + dim_date", { w: 190, h: 46 }),
]);

const backfill = box("backfill", "Pipeline_backfill_eod\nForEach (Sequential) per day", { w: 220, h: 46 });

// CI/CD lane — wheel goes through GitHub; the SPN does the deploy (no human hands)
const cicd = frame("cicd", "CI/CD — GitHub KaitoKid-123/ssv-data-platform (private)", { dir: "row", gap: 46, align: "middle", stroke: "#2da44e" }, [
  box("repo", "Dev ssv_data (local)\ncommit / PR", { w: 150, h: 46 }),
  box("ci", "CI — mỗi push/PR\npytest 24 tests + build wheel", { w: 200, h: 46 }),
  box("cd", "CD deploy.yml\nnút bấm / tag v* — pytest gate", { w: 200, h: 46 }),
  box("spn", "SPN spn-fabric-cicd\nworkspace Member\n(3 GitHub secrets)", { w: 160, h: 60 }),
  box("env", "Custom_Env\nstaging → publish", { w: 150, h: 46 }),
]);

// Notebook/model lane — the UI is the source of truth; git holds backup + restore/DR
const nblane = frame("nblane", "Notebooks / Model — dev trực tiếp trên Fabric UI (thin-shell rule)", { dir: "row", gap: 46, align: "middle", stroke: "#9673a6" }, [
  box("ui", "Sửa notebook / model / report\ntrên Fabric UI", { w: 190, h: 46 }),
  box("exp", "export_definitions.py\n→ fabric_items/ + manifest.json\n(backup có history)", { w: 210, h: 60 }),
  box("res", "deploy_definitions.py\nrestore in-place / DR sang ws mới\n(tự remap GUID)", { w: 220, h: 60 }),
  box("ver", "verify_run.py\nrun ngày idempotent + DAX diff\nvs baseline 30 ngày", { w: 200, h: 60 }),
]);

renderTree(d2, phantom("root2", "EOD Sales — orchestration + deploy + CI/CD", { dir: "col", gap: 44 }, [
  pipeline, backfill, cicd, nblane,
]), [40, 60]);

for (const a of ["cp1", "cp2", "cp3", "nbpg", "nbdlm"]) {
  d2.link("setv", a, "");
  d2.link(a, "tr", "");
}
d2.link("tr", "dq", "");
d2.link("dq", "birf", "Succeeded");
d2.link("backfill", "pipeline", "invoke per run_date", { dash: true });  // to the frame border, not through nodes
d2.link("repo", "ci", "push");
d2.link("ci", "cd", "merge / tag");
d2.link("cd", "spn", "secrets");
d2.link("spn", "env", "upload + publish");
d2.link("cd", "ver", "option verify", { dash: true });
d2.link("ui", "exp", "backup", { dash: true });
d2.link("exp", "res", "khi cần restore", { dash: true });

const r2 = d2.validate();
console.log("P2 VALIDATE:", JSON.stringify({ ok: r2.ok, errors: r2.errors, warnings: r2.warnings }));


// ---------------- Page 0: Simple overview (for everyone) ----------------
const d0 = new Diagram("network");

const src0 = frame("z_src", "Ngu\u1ed3n d\u1eef li\u1ec7u", { dir: "col", gap: 22, stroke: SRC }, [
  icon("z_mongo", "mongodb", "MongoDB Atlas"),
  icon("z_pg", "azure_azure_database_postgresql_server", "PostgreSQL"),
  icon("z_dlm", "azure_api_management_services", "DLM API"),
]);

const lake0 = frame("z_lake", "Lakehouse lh_eod_sales (OneLake)", { dir: "row", gap: 28, align: "middle", stroke: "#999999", cornerIcon: "azure_data_lake_storage_gen1" }, [
  box("z_bronze", "BRONZE\nD\u1eef li\u1ec7u th\u00f4", { w: 130, h: 70, fill: "#ffe6cc", stroke: BRZ }),
  box("z_silver", "SILVER\nL\u00e0m s\u1ea1ch + chu\u1ea9n h\u00f3a", { w: 160, h: 70, fill: "#f5f5f5", stroke: SLV }),
  box("z_gold", "GOLD\nB\u1ea3ng business\n(fact + marts)", { w: 150, h: 70, fill: "#fff2cc", stroke: GLD }),
]);

const fabric0 = frame("z_fabric", "Microsoft Fabric", { dir: "row", gap: 46, align: "middle", stroke: "#0078D4" }, [
  icon("z_pipe", "azure_data_factories", "Data Pipeline (daily)"),
  lake0,
  icon("z_pbi", "power_bi", "Power BI"),
]);

const users0 = box("z_users", "Business users\nDashboard 6 trang", { w: 160, h: 60 });

renderTree(d0, phantom("root0", "EOD Sales tr\u00ean Microsoft Fabric \u2014 t\u1ed5ng quan", { dir: "row", gap: 56, align: "middle" }, [
  src0, fabric0, users0,
]), [40, 60]);

d0.link("z_src", "z_pipe", "ingest m\u1ed7i ng\u00e0y");
d0.link("z_pipe", "z_bronze", "load");
d0.link("z_bronze", "z_silver", "");
d0.link("z_silver", "z_gold", "");
d0.link("z_gold", "z_pbi", "DQ pass \u2192 Direct Lake");
d0.link("z_pbi", "z_users", "");

const r0 = d0.validate();
console.log("P0 VALIDATE:", JSON.stringify({ ok: r0.ok, errors: r0.errors, warnings: r0.warnings }));


// ---------------- Page 3: Fabric platform map (canonical style, mapped to EOD Sales) ----------------
const d3 = new Diagram("network");
const TEAL = "#12A79D", TEALF = "#d5f0ee", PUR = "#7719AA", FADE = "#bbbbbb";

// Row 1 — workload cards (white cards, coloured icons; faded = not used yet)
const workloads = phantom("w_row", "", { dir: "row", gap: 34, align: "top" }, [
  frame("w_df", "", { dir: "col", gap: 4, stroke: "#dddddd", fill: "#ffffff" }, [
    icon("w_df_i", "azure_data_factories", "Data Factory\nPipeline_eod_sale_product")]),
  frame("w_de", "", { dir: "col", gap: 4, stroke: "#dddddd", fill: "#ffffff" }, [
    icon("w_de_i", "azure_azure_synapse_analytics", "Data Engineering\nNotebooks bronze/silver/gold")]),
  frame("w_pbi", "", { dir: "col", gap: 4, stroke: "#dddddd", fill: "#ffffff" }, [
    icon("w_pbi_i", "power_bi", "Power BI\nmodel Sales + dashboard")]),
]);

// Row 2 — serverless compute band (teal pills)
const compute = frame("c_band", "Serverless compute", { dir: "row", gap: 60, align: "middle", stroke: TEAL, fill: "#ffffff" }, [
  box("c_spark", "Spark", { w: 110, h: 44, fill: TEALF, stroke: TEAL, bold: true, round: true }),
  box("c_tsql", "T-SQL\n(SQL endpoint)", { w: 130, h: 44, fill: TEALF, stroke: TEAL, bold: true, round: true }),
  box("c_as", "Analysis Services\n(Direct Lake)", { w: 150, h: 44, fill: TEALF, stroke: TEAL, bold: true, round: true }),
]);

// Row 3 — OneLake band (lakehouse schemas as folders)
const onelake = frame("o_band", "OneLake \u2014 Lakehouse lh_eod_sales", { dir: "row", gap: 44, align: "middle", stroke: "#999999", fill: "#ffffff", cornerIcon: "azure_data_lake_storage_gen1" }, [
  box("o_bronze", "bronze\n(staging)", { w: 120, h: 52, fill: "#ffe6cc", stroke: BRZ }),
  box("o_silver", "silver\n(conformed + SCD2)", { w: 150, h: 52, fill: "#f5f5f5", stroke: SLV }),
  box("o_gold", "gold\n(fact + BI marts)", { w: 140, h: 52, fill: "#fff2cc", stroke: GLD }),
]);

const fabric3 = frame("f_frame", "Microsoft Fabric", { dir: "col", gap: 40, align: "center", stroke: "#0078D4" }, [
  workloads, compute, onelake,
]);

// Left — mirroring option (roadmap)
const mirror = frame("m_grp", "Mirroring (option prod)", { dir: "col", gap: 10, stroke: PUR }, [
  icon("m_pg", "azure_azure_database_postgresql_server", "PostgreSQL"),
  box("m_note", "thay JDBC/Airbyte\n(roadmap)", { w: 140, h: 40, stroke: FADE }),
]);

// Bottom — sources: ingest + shortcut (analog of "Shortcuts")
const sources3 = frame("s_grp", "Sources \u2014 ingest / shortcut", { dir: "row", gap: 56, align: "top", stroke: PUR }, [
  icon("s_mongo", "mongodb", "MongoDB Atlas"),
  icon("s_pg", "azure_azure_database_postgresql_server", "PostgreSQL (Aiven)"),
  icon("s_dlm", "azure_api_management_services", "DLM REST API"),
]);

renderTree(d3, phantom("root3", "EOD Sales tr\u00ean c\u00e1c Fabric compute engines", { dir: "col", gap: 46, align: "center" }, [
  phantom("top3", "", { dir: "row", gap: 50, align: "top" }, [phantom("m_wrap", "", { dir: "col", gap: 0, header: 0 }, [mirror]), fabric3]),
  sources3,
]), [40, 60]);

d3.link("w_df", "c_spark", "orchestrate", { dash: true });
d3.link("w_de", "c_spark", "");
d3.link("w_de", "c_tsql", "");
d3.link("w_pbi", "c_as", "");
d3.link("c_spark", "o_band", "read/write Delta");
d3.link("c_tsql", "o_band", "");
d3.link("c_as", "o_gold", "Direct Lake");
d3.link("s_mongo", "o_bronze", "Copy (windowed)", { dash: true });
d3.link("s_pg", "o_bronze", "JDBC dev / Shortcut prod", { dash: true });
// NOTE: the checked-in .drawio refines this edge by hand (waypoints -> bronze).
// The auto-router crosses the silver box if targeted at o_bronze, so the script
// keeps the safe band-border target. Do not blindly regen over manual polish.
d3.link("s_dlm", "o_band", "REST \u2192 bronze", { dash: true });
d3.link("m_pg", "o_band", "mirror", { dash: true });

const r3 = d3.validate();
console.log("P3 VALIDATE:", JSON.stringify({ ok: r3.ok, errors: r3.errors, warnings: r3.warnings }));

// ---------------- write single-page files (for validate/render), then merged ----------------
const S = "/tmp/claude-1000/-home-khang-Fabric-Platform-ssv-data-platform/bd3837a8-c86f-419a-a764-0dfd57d26481/scratchpad";
const x0 = d0.mxfile("Overview (simple)");
const x1 = d1.mxfile("Data Flow (Medallion)");
const x2 = d2.mxfile("Orchestration + Deploy");
const x3 = d3.mxfile("Fabric Platform Map");
writeFileSync(`${S}/flow_p0.drawio`, x0);
writeFileSync(`${S}/flow_p1.drawio`, x1);
writeFileSync(`${S}/flow_p2.drawio`, x2);
writeFileSync(`${S}/flow_p3.drawio`, x3);

// merge: later pages get an id namespace so ids stay unique across the file
const dia = (x) => x.match(/<diagram[\s\S]*<\/diagram>/)[0];
const ns = (x, p) => x.replace(/\b(id|source|target|parent)="([^"]*)"/g, (_, a, v) => `${a}="${p}${v}"`);
const pages = dia(x0) + ns(dia(x3), "pC_") + ns(dia(x1), "pA_") + ns(dia(x2), "pB_");
const merged = x0.replace(/<diagram[\s\S]*<\/diagram>/, pages);
writeFileSync(`${S}/eod-sales-flow-kit.drawio`, merged);
console.log("written flow_p0/p1/p2.drawio + merged (3 pages)");
