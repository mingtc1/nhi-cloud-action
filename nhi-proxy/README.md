# NHI download proxy

This directory preserves the production `nhi-proxy` Worker as source-controlled
code. The Worker authenticates `/download`, limits successful downloads to one
per six hours, and streams the official NHI CSV without writing to D1.
Authenticated workflow diagnostics can send `X-Diagnostic-Dry-Run: 1` so a
successful preview test does not consume the production six-hour window.

The production workflow remains on GitHub-hosted runners; no user computer is
part of the synchronization path. The proxy is the primary download path, with
direct download from the macOS GitHub-hosted runner as the fallback. Diagnostics on
2026-09-28 found that the official NHI source reset direct GitHub-hosted runner
connections, while Cloudflare-to-Cloudflare requests returned upstream HTTP
520. A hostname placement hint was tested and removed because the official
hostname is anycast and the Worker still ran in a United States data center.

Do not switch the workflow to a self-hosted personal computer as a workaround.
Resolve the source-download route with a cloud-hosted egress path before this
branch is merged into the scheduled production workflow.

Use the `nhi_download_only` workflow input to exercise both download paths and
write a diagnostic report without reading from or writing to D1.

Upload a preview version first:

```powershell
npx wrangler versions upload --config nhi-proxy/wrangler.toml
```

Only deploy that version after its authenticated `/download` check succeeds.
