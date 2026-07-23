# logsink android client

Kotlin library implementing the ADR-011 client contract as a
[Timber](https://github.com/JakeWharton/timber) tree. Call sites keep using
plain `Timber.d/w/e` — this tree buffers, batches and ships according to the
contract (bounded drop-oldest buffer, interval flush, server-configured
level, 429/5xx backoff, 401 drop).

## Usage

```kotlin
// Application.onCreate()
val client = LogsinkClient(
    ingestUrl = "https://applogs.<your-domain>/ingest",
    apiKey = BuildConfig.LOGSINK_KEY,   // injected at build time, see below
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
- The client never logs through Timber (that would recurse via this tree);
  its own diagnostics go to logcat, sparsely.

## Status

Scaffold, not yet exercised by a real consumer — Retro FM is the first.
Compiles against AGP 8.7 / Kotlin 2.1; CI runs `:android:assembleRelease`.
