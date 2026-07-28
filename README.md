# logsink-clients

Client libraries for the `applogs` log sink — field-debug logging from apps
that cannot be reached any other way (a car head unit, a friend's phone, a
user's browser).

The server side lives elsewhere, by design (the GitOps split: app repos build,
`home-server` is the deployment truth):

- **Design + policy**: `home-server` repo, `docs/decisions/011-app-log-sink.md`
  (ADR-011). That document owns the wire contract; this README restates it for
  implementers, but ADR-011 wins on conflict.
- **Ingest edge**: [MagTer/logsink-shim](https://github.com/MagTer/logsink-shim)
  — per-app bearer keys, server-side app stamping, token buckets.
- **Storage**: VictoriaLogs (`applogs` class instance), queried via the
  Entra-gated vmui at the sink's hostname.

## What belongs here — and what does not

A client belongs here when the *device running it* is unreachable for normal
debugging: Android phones/Automotive builds, browsers on other people's
machines. One directory per language/platform, created **only when a real
consumer exists** — no empty scaffolds for imagined futures.

**Server-side apps do NOT log here.** Anything running on the home-server
platform (price-tracker, hermes, …) already has centrally reachable logs
(`scripts/logs.sh`, Dokploy, journald) and belongs to a different source
class — shipping them into the apps sink would let a chatty server app rotate
out field logs that cannot be re-captured (shared class disk cap, ADR-011).

## The wire contract (summary of ADR-011)

- `POST /ingest` with `Authorization: Bearer <per-app key>` — body is NDJSON,
  one JSON object per line: `{"ts": <epoch-ms or RFC3339>, "level":
  "DEBUG|INFO|WARN|ERROR", "tag": "...", "msg": "..."}`. Only `msg` is
  required; extra fields (the android client sends `device`, `sid` — a random
  per-process session id — and `seq`, a per-line sequence number for gap
  detection) pass through to the store. The app identity is stamped
  server-side from the key — clients never send (and cannot spoof) an app
  name.
- `GET /ingest/config` (same auth) → `{"app": ..., "level": ...}` — the level
  the server wants. Drop everything below it client-side; the shim drops
  again server-side as defense in depth.
- **Buffer bounded, drop-oldest, never unbounded on-device.** Batch and flush
  on an interval and on app-background; never block the UI thread.
- **Back off** on `429` (honor `Retry-After`) and on `5xx`/network errors
  (exponential, capped). On `401` drop the batch — the key is wrong, retrying
  cannot fix it.
- **Log hygiene is part of the contract**: once lines leave the device — no
  tokens, no URLs with credentials, no PII. Review call sites against this
  list, not just the transport.
- Keys are rate-limit/revocation handles, not secrets (they ship inside APKs
  and are extractable — accepted in ADR-011). Still: inject via build config,
  never hardcode in source.

## Layout

| Directory | Platform | Status |
|-----------|----------|--------|
| `android/` | Android / Android Automotive (Kotlin, Timber tree) | scaffold — first consumer: Retro FM |

A future `js/` (browser field-logging) additionally requires CORS support in
logsink-shim (Origin allowlist) — that shim change gets its own review before
the client exists.

## Versioning

Tag per package: `android-v0.1.0`, `js-v0.1.0`, … Packages release
independently; there is no repo-wide version.
