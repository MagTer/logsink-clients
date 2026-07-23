package se.falle.logsink

import android.util.Log
import timber.log.Timber

/**
 * Timber tree that forwards to a [LogsinkClient]. Plant it beside (not
 * instead of) a DebugTree — call sites keep using plain Timber:
 *
 *     val client = LogsinkClient(
 *         ingestUrl = "https://applogs.example.com/ingest",
 *         apiKey = BuildConfig.LOGSINK_KEY,
 *     )
 *     Timber.plant(LogsinkTree(client))
 *
 * Hygiene is part of the wire contract (ADR-011): once lines leave the
 * device — no tokens, no URLs with credentials, no PII. That is enforced at
 * call sites, not here; review against the contract.
 */
class LogsinkTree(private val client: LogsinkClient) : Timber.Tree() {

    override fun log(priority: Int, tag: String?, message: String, t: Throwable?) {
        val level = when {
            priority >= Log.ERROR -> "ERROR"
            priority == Log.WARN -> "WARN"
            priority == Log.INFO -> "INFO"
            else -> "DEBUG"
        }
        val msg = if (t != null) message + "\n" + Log.getStackTraceString(t) else message
        client.enqueue(level, tag, msg)
    }
}
