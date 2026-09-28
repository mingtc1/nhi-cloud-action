# NHI download proxy

This directory preserves the production `nhi-proxy` Worker as source-controlled
code. The Worker authenticates `/download`, limits successful downloads to one
per six hours, and streams the official NHI CSV without writing to D1.
Authenticated workflow diagnostics can send `X-Diagnostic-Dry-Run: 1` so a
successful preview test does not consume the production six-hour window.

The placement hint runs the Worker close to `info.nhi.gov.tw`. This avoids
routing the origin request from the GitHub runner's nearest Cloudflare location,
which returned upstream HTTP 520 during the 2026-09-28 diagnostic run.

Upload a preview version first:

```powershell
npx wrangler versions upload --config nhi-proxy/wrangler.toml
```

Only deploy that version after its authenticated `/download` check succeeds.
