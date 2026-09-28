# NHI download proxy

This directory preserves the production `nhi-proxy` Worker as source-controlled
code. The Worker authenticates `/download`, limits successful downloads to one
per six hours, and streams the official NHI CSV without writing to D1.
Authenticated workflow diagnostics can send `X-Diagnostic-Dry-Run: 1` so a
successful preview test does not consume the production six-hour window.

The proxy remains a fallback. The primary NHI download runs on the Taiwan
self-hosted runner because Cloudflare-to-Cloudflare requests to the official
source returned upstream HTTP 520. A hostname placement hint was tested and
removed because the official hostname is anycast and the Worker still ran in a
United States data center.

Upload a preview version first:

```powershell
npx wrangler versions upload --config nhi-proxy/wrangler.toml
```

Only deploy that version after its authenticated `/download` check succeeds.
