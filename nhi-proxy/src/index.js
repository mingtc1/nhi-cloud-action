const NHI_CSV_URL =
  "https://info.nhi.gov.tw/api/iode0000s01/Dataset?rId=A21030000I-E41001-001";
const MIN_INTERVAL_SECONDS = 6 * 60 * 60;
const RATE_LIMIT_KEY = "last_download_ts";

function jsonResponse(payload, status, extraHeaders = {}) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "Content-Type": "application/json",
      ...extraHeaders,
    },
  });
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/health") {
      return jsonResponse(
        {
          status: "ok",
          worker_colo: request.cf?.colo || "unknown",
        },
        200,
      );
    }

    if (url.pathname !== "/download") {
      return new Response("Not Found", { status: 404 });
    }

    const authHeader = request.headers.get("Authorization") || "";
    const bearerToken = authHeader.startsWith("Bearer ")
      ? authHeader.slice(7)
      : url.searchParams.get("token") || "";

    if (!env.PROXY_TOKEN || bearerToken !== env.PROXY_TOKEN) {
      return new Response("Unauthorized", { status: 401 });
    }

    const isDiagnostic = request.headers.get("X-Diagnostic-Dry-Run") === "1";

    if (env.RATE_LIMIT_KV && !isDiagnostic) {
      const lastTs = await env.RATE_LIMIT_KV.get(RATE_LIMIT_KEY);
      if (lastTs) {
        const elapsed = Math.floor(Date.now() / 1000) - Number.parseInt(lastTs, 10);
        if (elapsed < MIN_INTERVAL_SECONDS) {
          const retryAfter = MIN_INTERVAL_SECONDS - elapsed;
          return jsonResponse(
            {
              error: "Rate limited",
              retry_after_seconds: retryAfter,
              message: `Last download was ${Math.floor(elapsed / 60)} minutes ago. Minimum interval is ${MIN_INTERVAL_SECONDS / 3600} hours.`,
            },
            429,
            { "Retry-After": String(retryAfter) },
          );
        }
      }
    }

    let nhiFetch;
    try {
      nhiFetch = await fetch(NHI_CSV_URL, {
        headers: {
          "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
          Accept: "text/csv,*/*",
        },
        cf: {
          cacheTtl: 0,
          cacheEverything: false,
        },
      });
    } catch (error) {
      return jsonResponse(
        {
          error: "NHI fetch failed",
          detail: String(error),
          worker_colo: request.cf?.colo || "unknown",
        },
        502,
      );
    }

    if (!nhiFetch.ok) {
      return jsonResponse(
        {
          error: "NHI upstream error",
          status: nhiFetch.status,
          statusText: nhiFetch.statusText || "<none>",
          worker_colo: request.cf?.colo || "unknown",
          upstream_cf_ray: nhiFetch.headers.get("CF-Ray") || "unknown",
          upstream_server: nhiFetch.headers.get("Server") || "unknown",
        },
        502,
      );
    }

    if (env.RATE_LIMIT_KV && !isDiagnostic) {
      await env.RATE_LIMIT_KV.put(
        RATE_LIMIT_KEY,
        String(Math.floor(Date.now() / 1000)),
        { expirationTtl: MIN_INTERVAL_SECONDS * 2 },
      );
    }

    const headers = {
      "Content-Type":
        nhiFetch.headers.get("Content-Type") || "text/csv; charset=utf-8",
      "X-Proxy-Source": "nhi-proxy",
      "X-Download-Time": new Date().toISOString(),
      "X-Worker-Colo": request.cf?.colo || "unknown",
    };
    const contentLength = nhiFetch.headers.get("Content-Length");
    if (contentLength) headers["Content-Length"] = contentLength;

    return new Response(nhiFetch.body, { status: 200, headers });
  },
};
