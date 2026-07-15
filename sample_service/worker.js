// Cloudflare Worker equivalent of the DLM mock (api_service.py).
// Same response shape: a flat JSON array, exactly what
// spark.createDataFrame(resp.json()) in bronze_ingest expects.
//
// Why this over FastAPI-on-free-tier: Workers are always-on with no cold start,
// so a scheduled Fabric run never hits a 30–60s wake-up that would blow the
// requests timeout=20 on the DLM call.
//
// Deploy:
//   npm i -g wrangler   (or: npx wrangler ...)
//   wrangler deploy     -> gives you https://<name>.<subdomain>.workers.dev
// Or paste into Cloudflare dashboard > Workers & Pages > Create > Quick edit.

const PARTNER_SOURCES = [
  { partner_source: "src1", name: "GrabExpress" },
  { partner_source: "src2", name: "AhaMove" },
];

export default {
  async fetch(request) {
    const { pathname } = new URL(request.url);
    if (pathname === "/api/hq/dlm/partner_sources") return Response.json(PARTNER_SOURCES);
    if (pathname === "/health") return Response.json({ status: "ok" });
    return new Response("not found", { status: 404 });
  },
};
