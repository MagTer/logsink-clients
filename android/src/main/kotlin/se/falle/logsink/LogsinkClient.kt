package se.falle.logsink

import android.util.Log
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.json.JSONObject
import java.io.IOException
import java.net.HttpURLConnection
import java.net.URL
import java.util.ArrayDeque

/**
 * Ships log lines to a logsink-shim instance per the ADR-011 client contract:
 *
 *  - bounded buffer, drop-oldest — never unbounded on-device
 *  - batch NDJSON POST /ingest on an interval, off the main thread
 *  - level fetched from GET /ingest/config and cached; lines below it are
 *    dropped client-side (the shim drops again server-side)
 *  - 429 honors Retry-After; 5xx/network errors back off exponentially
 *    (capped); 401 drops the batch — a wrong key cannot be retried into
 *    working
 *
 * This class must never log through Timber itself (a LogsinkTree would loop);
 * its own diagnostics go to logcat only, and sparsely.
 */
class LogsinkClient(
    /** Full ingest URL, e.g. "https://applogs.example.com/ingest". */
    private val ingestUrl: String,
    /** Per-app append key. Inject via BuildConfig — never hardcode in source. */
    private val apiKey: String,
    /** Level used until /ingest/config has answered. */
    defaultLevel: String = "WARN",
    private val flushIntervalMs: Long = 15_000L,
    private val configRefreshMs: Long = 5 * 60_000L,
    private val maxBufferedLines: Int = 2_000,
    private val maxBatchLines: Int = 200,
    private val maxBackoffMs: Long = 5 * 60_000L,
    scope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.IO),
) {
    private companion object {
        const val TAG = "LogsinkClient"
        val LEVELS = mapOf("DEBUG" to 10, "INFO" to 20, "WARN" to 30, "ERROR" to 40)
    }

    private val buffer = ArrayDeque<String>()
    private val lock = Any()

    @Volatile
    private var minLevel: Int = LEVELS[defaultLevel.uppercase()] ?: 30

    @Volatile
    private var backoffUntilMs: Long = 0L
    private var backoffMs: Long = 0L
    private var loggedAuthFailure = false

    private val flushJob: Job = scope.launch {
        while (true) {
            delay(flushIntervalMs)
            runCatching { flushInternal() }
        }
    }
    private val configJob: Job = scope.launch {
        while (true) {
            runCatching { refreshConfig() }
            delay(configRefreshMs)
        }
    }

    /** Called by [LogsinkTree]; safe from any thread, never blocks on I/O. */
    fun enqueue(level: String, tag: String?, msg: String) {
        val levelValue = LEVELS[level] ?: 20
        if (levelValue < minLevel) return
        val line = JSONObject().apply {
            put("ts", System.currentTimeMillis())
            put("level", level)
            if (tag != null) put("tag", tag)
            put("msg", msg)
        }.toString()
        synchronized(lock) {
            if (buffer.size >= maxBufferedLines) buffer.pollFirst() // drop-oldest
            buffer.addLast(line)
        }
    }

    /**
     * Flush outside the interval — call from a lifecycle hook when the app
     * goes to background (e.g. ProcessLifecycleOwner ON_STOP).
     */
    suspend fun flush() = withContext(Dispatchers.IO) { runCatching { flushInternal() } }

    private fun flushInternal() {
        if (System.currentTimeMillis() < backoffUntilMs) return
        while (true) {
            val batch = synchronized(lock) {
                val n = minOf(maxBatchLines, buffer.size)
                if (n == 0) return
                List(n) { buffer.pollFirst()!! }
            }
            when (val result = post(batch.joinToString("\n"))) {
                is SendResult.Ok -> {
                    backoffMs = 0L
                    loggedAuthFailure = false
                }
                is SendResult.RetryLater -> {
                    // Put the batch back at the FRONT so ordering survives.
                    synchronized(lock) {
                        batch.asReversed().forEach { buffer.addFirst(it) }
                        while (buffer.size > maxBufferedLines) buffer.pollFirst()
                    }
                    backoffMs = if (result.retryAfterMs > 0) result.retryAfterMs
                    else minOf(if (backoffMs == 0L) flushIntervalMs else backoffMs * 2, maxBackoffMs)
                    backoffUntilMs = System.currentTimeMillis() + backoffMs
                    return
                }
                is SendResult.Drop -> {
                    if (!loggedAuthFailure) {
                        Log.w(TAG, "ingest rejected the app key (401) — dropping batch, check the configured key")
                        loggedAuthFailure = true
                    }
                }
            }
        }
    }

    private sealed class SendResult {
        object Ok : SendResult()
        object Drop : SendResult()
        data class RetryLater(val retryAfterMs: Long) : SendResult()
    }

    private fun post(ndjson: String): SendResult {
        return try {
            val conn = (URL(ingestUrl).openConnection() as HttpURLConnection).apply {
                requestMethod = "POST"
                connectTimeout = 10_000
                readTimeout = 10_000
                doOutput = true
                setRequestProperty("Authorization", "Bearer $apiKey")
                setRequestProperty("Content-Type", "application/x-ndjson")
            }
            conn.outputStream.use { it.write(ndjson.toByteArray(Charsets.UTF_8)) }
            val code = conn.responseCode
            val retryAfterMs = (conn.getHeaderField("Retry-After")?.toLongOrNull() ?: 0L) * 1000L
            conn.disconnect()
            when {
                code in 200..299 -> SendResult.Ok
                code == 401 -> SendResult.Drop
                code == 429 -> SendResult.RetryLater(retryAfterMs)
                else -> SendResult.RetryLater(0L)
            }
        } catch (_: IOException) {
            SendResult.RetryLater(0L)
        }
    }

    private fun refreshConfig() {
        try {
            val conn = (URL("$ingestUrl/config").openConnection() as HttpURLConnection).apply {
                requestMethod = "GET"
                connectTimeout = 10_000
                readTimeout = 10_000
                setRequestProperty("Authorization", "Bearer $apiKey")
            }
            if (conn.responseCode in 200..299) {
                val body = conn.inputStream.use { it.readBytes().toString(Charsets.UTF_8) }
                val level = JSONObject(body).optString("level", "").uppercase()
                LEVELS[level]?.let { minLevel = it }
            }
            conn.disconnect()
        } catch (_: IOException) {
            // Keep the current level; next refresh retries.
        }
    }
}
