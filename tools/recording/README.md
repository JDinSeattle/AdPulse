# Record the running local stack

```bash
npm ci --prefix tools/recording
npx --prefix tools/recording playwright install --with-deps chromium
node tools/recording/record.mjs
```

Requires the running local Docker stack and its default Grafana demo account. The script starts a fresh browser context, waits for actual charts/jobs, and captures about one minute of real UI at 1440×1000. It writes screenshots, a chapter index and WebM to `artifacts/recording/`. No production/browser-profile data is used. Screenshots and chapters must be inspected before sharing.

Optionally set `ADPULSE_CHROME_PATH` to an existing Chrome executable. Convert the resulting WebM with `ffmpeg -i <video.webm> -c:v libx264 -crf 24 -pix_fmt yuv420p -movflags +faststart <demo.mp4>`. A recording demonstrates the UI; quantitative claims come from the separate machine-readable test reports.
