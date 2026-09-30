# logsink android client

Kotlin library implementing the ADR-011 client contract as a
[Timber](https://github.com/JakeWharton/timber) tree. Call sites keep using
plain `Timber.d/w/e` — this tree buffers, batches and ships according to the
contract (bounded drop-oldest buffer, interval flush, server-configured
level, 429/5xx backoff, 401 drop; a 3xx is never followed and is dropped like a 401,
since that is how Cloudflare Access refuses a request).

## Usage

```kotlin
// Application.onCreate()
val client = LogsinkClient(
    ingestUrl = "https://applogs.<your-domain>/ingest",
    apiKey = BuildConfig.LOGSINK_KEY,   // injected at build time, see below
    // Only behind Cloudflare Access (home-server's cluster): its service token, both or
    // neither. Blank = not sent, so a build without them works against a sink without Access.
    accessClientId = BuildConfig.LOGSINK_CF_ID,
    accessClientSecret = BuildConfig.LOGSINK_CF_SECRET,
)
Timber.plant(LogsinkTree(client))
```

Optional but recommended — flush when the app leaves the foreground
(requires `androidx.lifecycle:lifecycle-process` in the app):

```kotlin
ProcessLifecycleOwner.get().lifecycle.addObserver(LifecycleEventObserver { _, event ->
    if (event == Lifecycle.Event.ON_STOP) {
        ProcessLifecycleOwner.get().lifecycleScope.launch { client.flush() }
    }
})
```

## Key injection

The append key is a rate-limit/revocation handle, not a secret (it ships in
the APK and is extractable — accepted in ADR-011). It still never belongs in
source:

```kotlin
// app/build.gradle.kts
android {
    defaultConfig {
        buildConfigField("String", "LOGSINK_KEY",
            "\"${project.findProperty("logsinkKey") ?: ""}\"")
    }
    buildFeatures { buildConfig = true }
}
```

Then `-PlogsinkKey=...` in CI or `logsinkKey=...` in the untracked
`~/.gradle/gradle.properties`.

## Behavior details

- Lines below the server-configured level are dropped at enqueue time; the
  level refreshes from `GET /ingest/config` every 5 min (default `WARN`
  until the first answer, matching the sink's steady state).
- The buffer holds max 2 000 lines; overflow drops the OLDEST lines first —
  during a long offline stretch (a car without coverage) you keep the newest
  context, and memory never grows unbounded.
- `429` pauses sending for `Retry-After`; network errors and `5xx` back off
  exponentially, capped at 5 min. `401` drops the batch and logs one warning
  to logcat — a wrong key never causes a retry storm.
- `413` halves the batch and retries, restoring the full size on the next
  success; a single line that still will not fit is dropped. A batch is bounded
  by bytes (128 KB) as well as lines, and one message is truncated at 8 KB.
  This exists because a body limit is usually enforced by a proxy the client
  was never told about: a reverse proxy in front of one sink capped bodies at
  4 KB, and before this the client retried the same oversized batch forever —
  every line behind it stuck for the life of the process, which looks exactly
  like an app that stopped logging (root-caused 2026-08-09). **Any permanent
  rejection has to make the queue smaller, never leave it unchanged.**
- The client never logs through Timber (that would recurse via this tree);
  its own diagnostics go to logcat, sparsely.

## Durable spool (opt-in)

The in-memory buffer survives an offline stretch, but not the process dying
during one — the tail of a drive that ends out of coverage is lost. Pass a
`spoolFile` to persist it:

```kotlin
val client = LogsinkClient(
    ingestUrl = "...",
    apiKey = BuildConfig.LOGSINK_KEY,
    spoolFile = File(filesDir, "logsink-spool.ndjson"),
    spoolMinWriteIntervalMs = 120_000L,   // raise on flash-sensitive hardware
)
// Startup, off the main thread — restores a previous process's unshipped lines.
scope.launch { client.replaySpool() }
// Teardown hooks (ON_STOP, Service.onDestroy, uncaught-exception handler).
scope.launch { client.persistNow() }
```

It is a last resort, not a mirror:

- **The logging path never touches disk.** A fully online session writes
  nothing at all — the file is only created once a flush has actually failed.
- Automatic writes are rate-limited by `spoolMinWriteIntervalMs` (default
  2 min) and skipped entirely when nothing has been logged since the last one,
  so an idle offline stretch costs zero writes however long it lasts.
- The file is a whole-file rewrite capped at `spoolMaxBytes` (default 64 KB),
  newest lines kept — it cannot grow, and it is written via temp + rename so a
  kill mid-write cannot tear it.
- It is deleted as soon as the buffer ships. Steady state is no file.
- `replaySpool()` deletes the file *before* consuming it and caps what it
  restores (`spoolMaxReplayLines`), so a backlog can neither survive a crashing
  replay nor evict the session that is about to happen.
- Any I/O failure disables the spool for the process and logs one line to
  logcat. **It must never be able to take logging down with it** — an earlier
  consumer-side spool appended every line synchronously on the logging thread
  and replayed a growing backlog on the main thread at boot; the app ANR'd, was
  killed, and each restart had more to replay. Logging died completely. The
  constraints above exist to make that shape impossible.

## Status

Scaffold, not yet exercised by a real consumer — Retro FM is the first.
Compiles against AGP 8.7 / Kotlin 2.1; CI runs `:android:assembleRelease`.
