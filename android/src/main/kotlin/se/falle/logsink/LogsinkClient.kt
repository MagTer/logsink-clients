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
import java.io.File
import java.io.IOException
import java.net.HttpURLConnection
import java.net.URL
import java.util.ArrayDeque
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

/**
 * Ships log lines to a logsink-shim instance per the ADR-011 client contract:
 *
 *  - bounded buffer, drop-oldest — never unbounded on-device; overflow is
 *    accounted for with a synthetic "dropped N lines" WARN marker so a gap in
 *    the sink is distinguishable from an app that logged nothing
 *  - every line carries a random per-process session id ("sid") and a
 *    monotonically increasing sequence number ("seq") — process restarts and
 *    shipping gaps are visible as sid changes / seq holes
 *  - batch NDJSON POST /ingest on an interval, off the main thread
 *  - level fetched from GET /ingest/config and cached; lines below it are
 *    dropped client-side (the shim drops again server-side)
 *  - 429 honors Retry-After; 5xx/network errors back off exponentially
 *    (capped); 401 drops the batch — a wrong key cannot be retried into
 *    working
 *  - [flushNow] flushes past any accumulated backoff — hook it to a
 *    validated-connectivity signal so a backoff grown while offline cannot
 *    sleep through a short online window
 *  - optional durable spool ([spoolFile]) so the buffer survives a process
 *    death that happens while offline — see "Spool" below
 *
 * This class must never log through Timber itself (a LogsinkTree would loop);
 * its own diagnostics go to logcat only, and sparsely.
 *
 * ## Spool
 *
 * Off unless [spoolFile] is set. It is a last resort, **not** a mirror: the
 * logging path never touches disk, and a fully online session writes nothing at
 * all. Disk is touched only when a flush has actually failed (rate-limited by
 * [spoolMinWriteIntervalMs]) and when the host calls [persistNow] on the way
 * down. The file is a whole-file rewrite capped at [spoolMaxBytes], so it cannot
 * grow, and it is deleted as soon as the buffer ships — steady state is no file.
 *
 * This shape is deliberate. An earlier spool in a consumer app appended every
 * line synchronously on the logging thread and replayed an ever-growing backlog
 * on the main thread at startup; the app ANR'd during boot, was killed, and the
 * next boot had more to replay — logging died completely and got worse each
 * restart. Hence: no per-line I/O, a hard size cap, replay off the main thread
 * with the file deleted *before* it is consumed, a replay cap so old lines
 * cannot evict the live session, and any I/O failure disabling the spool for the
 * process rather than being retried. **The spool must never be able to take
 * logging down with it.**
 */
class LogsinkClient(
    /** Full ingest URL, e.g. "https://applogs.example.com/ingest". */
    private val ingestUrl: String,
    /** Per-app append key. Inject via BuildConfig — never hardcode in source. */
    private val apiKey: String,
    /** Cloudflare Access service token in front of the sink (home-server
     *  APPLOGS-MIGRATION-DESIGN A4). Sent as CF-Access-Client-Id/-Secret only when BOTH are
     *  non-blank, so an installation without them behaves exactly as before. Like the key it
     *  ships in the APK and is extractable; it only gets a request past Access. */
    private val accessClientId: String? = null,
    private val accessClientSecret: String? = null,
    /** Optional device label (e.g. Build.MODEL) — lets one app's phone/car/tablet
     *  lines be told apart in the sink. */
    private val device: String? = null,
    /** Level used once /ingest/config has answered; see [configKnown] for before. */
    defaultLevel: String = "WARN",
    private val flushIntervalMs: Long = 15_000L,
    private val configRefreshMs: Long = 5 * 60_000L,
    /** Retry cadence until the FIRST config answer — a car may be offline for the
     *  first minutes of a drive, and 5 min would miss the interesting window. */
    private val configInitialRetryMs: Long = 30_000L,
    private val maxBufferedLines: Int = 2_000,
    private val maxBatchLines: Int = 200,
    /** Byte budget for one POST body. A line cap alone says nothing about size — 200 lines
     *  carrying long messages is a very different request from 200 short ones, and the entity
     *  that rejects an oversized body is usually a proxy the client never hears about in
     *  advance. Keep this below the smallest body limit on the path. */
    private val maxBatchBytes: Int = 128 * 1024,
    /** Longest single message kept intact; the rest is truncated with a marker. A line larger
     *  than the path's body limit could never ship in any batch, so it must not exist. */
    private val maxLineBytes: Int = 8 * 1024,
    private val maxBackoffMs: Long = 5 * 60_000L,
    /** Durable spool for lines that could not ship before the process died. Null = off.
     *  Pass a file in the app's private storage, e.g. File(filesDir, "logsink-spool.ndjson"). */
    private val spoolFile: File? = null,
    /** Hard cap on the spool file. Newest lines win; the file is rewritten, never appended. */
    private val spoolMaxBytes: Int = 64 * 1024,
    /** Floor between spool writes while offline. The only knob that bounds write frequency —
     *  raise it on flash-sensitive hardware (car head units), never lower it below a few
     *  tens of seconds. */
    private val spoolMinWriteIntervalMs: Long = 120_000L,
    /** Cap on lines restored by [replaySpool], so a backlog cannot evict the live session. */
    private val spoolMaxReplayLines: Int = 500,
    scope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.IO),
) {
    private companion object {
        const val TAG = "LogsinkClient"
        val LEVELS = mapOf("DEBUG" to 10, "INFO" to 20, "WARN" to 30, "ERROR" to 40)
    }

    private val accessHeaders: Boolean =
        !accessClientId.isNullOrBlank() && !accessClientSecret.isNullOrBlank()

    /** Every request carries the same credentials, set in one place. Redirects are NOT
     *  followed: Access answers a refused request with a 302 to its login page, and a
     *  followed redirect would read that page's 200 as "shipped" and drop the lines. */
    private fun HttpURLConnection.authorize() {
        instanceFollowRedirects = false
        setRequestProperty("Authorization", "Bearer $apiKey")
        if (accessHeaders) {
            setRequestProperty("CF-Access-Client-Id", accessClientId)
            setRequestProperty("CF-Access-Client-Secret", accessClientSecret)
        }
    }

    private val buffer = ArrayDeque<String>()
    private val lock = Any()

    /** Random per-process id + per-line sequence: sid changes mark restarts, seq holes mark
     *  lost lines — the difference between "app went silent" and "lines were dropped". */
    private val sessionId = UUID.randomUUID().toString().take(8)
    private val seq = AtomicLong(0L)

    /** Lines discarded by drop-oldest since the last overflow marker. Guarded by [lock]. */
    private var droppedSinceMarker = 0L

    @Volatile
    private var minLevel: Int = LEVELS[defaultLevel.uppercase()] ?: 30

    /** False until /ingest/config has answered once. Until then EVERYTHING is
     *  enqueued (bounded buffer; the shim still drops below-level server-side) —
     *  otherwise an offline app start silently discards the lines an
     *  investigation turned DEBUG on to capture. */
    @Volatile
    private var configKnown = false

    @Volatile
    private var backoffUntilMs: Long = 0L
    private var backoffMs: Long = 0L
    private var loggedAuthFailure = false

    /** Lines per POST, halved whenever the path answers 413 and restored on the next success.
     *  Lets the client discover a body limit it was never told about. */
    private var batchLineCap: Int = maxBatchLines

    /** Set on the first spool I/O failure and never cleared: a broken or full filesystem must
     *  degrade to "no spool", not to a write attempt on every flush. */
    @Volatile
    private var spoolDisabled = false
    private var lastSpoolWriteMs = 0L

    /** [seq] at the last spool write. Nothing logged since means nothing new to persist, so
     *  an idle offline stretch costs zero writes however long it lasts. */
    private var lastSpooledSeq = 0L

    private val flushJob: Job = scope.launch {
        while (true) {
            delay(flushIntervalMs)
            runCatching { flushInternal() }
        }
    }
    private val configJob: Job = scope.launch {
        while (true) {
            runCatching { refreshConfig() }
            delay(if (configKnown) configRefreshMs else configInitialRetryMs)
        }
    }

    /** Called by [LogsinkTree]; safe from any thread, never blocks on I/O. */
    fun enqueue(level: String, tag: String?, msg: String) {
        val levelValue = LEVELS[level] ?: 20
        if (configKnown && levelValue < minLevel) return
        val line = jsonLine(level, tag, msg)
        synchronized(lock) {
            if (buffer.size >= maxBufferedLines) { // drop-oldest
                buffer.pollFirst()
                droppedSinceMarker++
            }
            buffer.addLast(line)
        }
    }

    private fun jsonLine(level: String, tag: String?, msg: String): String =
        JSONObject().apply {
            put("ts", System.currentTimeMillis())
            put("level", level)
            if (tag != null) put("tag", tag)
            if (device != null) put("device", device)
            put("sid", sessionId)
            put("seq", seq.incrementAndGet())
            put("msg", if (msg.length <= maxLineBytes) msg else msg.take(maxLineBytes) + "…[truncated]")
        }.toString()

    /**
     * Flush outside the interval — call from a lifecycle hook when the app
     * goes to background (e.g. ProcessLifecycleOwner ON_STOP).
     */
    suspend fun flush() = withContext(Dispatchers.IO) { runCatching { flushInternal() } }

    /**
     * Flush immediately, ignoring any accumulated backoff — call when connectivity is known
     * to have just returned (e.g. NET_CAPABILITY_VALIDATED). A backoff grown during an
     * offline stretch would otherwise sleep through a short online window.
     */
    suspend fun flushNow() = withContext(Dispatchers.IO) {
        backoffMs = 0L
        backoffUntilMs = 0L
        runCatching { flushInternal() }
    }

    private fun flushInternal() {
        if (System.currentTimeMillis() < backoffUntilMs) return
        while (true) {
            val batch = synchronized(lock) {
                // Account for overflow before the batch, so the marker ships ahead of the
                // lines that survived it.
                if (droppedSinceMarker > 0) {
                    buffer.addFirst(
                        jsonLine("WARN", TAG, "buffer overflow: dropped $droppedSinceMarker lines on device")
                    )
                    droppedSinceMarker = 0
                }
                if (buffer.isEmpty()) return
                // Bounded by lines AND bytes; batchLineCap shrinks when something on the path
                // rejects the size, so the queue can find a size that fits instead of
                // retrying an impossible request forever.
                val take = ArrayList<String>()
                var bytes = 0
                while (take.size < minOf(batchLineCap, buffer.size)) {
                    val next = buffer.peekFirst() ?: break
                    val cost = next.length + 1
                    if (take.isNotEmpty() && bytes + cost > maxBatchBytes) break
                    buffer.pollFirst()
                    take.add(next)
                    bytes += cost
                }
                take
            }
            when (val result = post(batch.joinToString("\n"))) {
                is SendResult.Ok -> {
                    backoffMs = 0L
                    batchLineCap = maxBatchLines
                    loggedAuthFailure = false
                    // Everything that was at risk has now shipped: drop the spool so the
                    // steady state is no file on disk and the next boot replays nothing.
                    if (synchronized(lock) { buffer.isEmpty() }) clearSpool()
                }
                is SendResult.RetryLater -> {
                    // Put the batch back at the FRONT so ordering survives.
                    synchronized(lock) {
                        batch.asReversed().forEach { buffer.addFirst(it) }
                        while (buffer.size > maxBufferedLines) {
                            buffer.pollFirst()
                            droppedSinceMarker++
                        }
                    }
                    backoffMs = if (result.retryAfterMs > 0) result.retryAfterMs
                    else minOf(if (backoffMs == 0L) flushIntervalMs else backoffMs * 2, maxBackoffMs)
                    backoffUntilMs = System.currentTimeMillis() + backoffMs
                    // The lines are now provably at risk — this is the only automatic path to
                    // disk, and it is rate-limited. Already on Dispatchers.IO here.
                    maybeSpool(force = false)
                    return
                }
                is SendResult.Drop -> {
                    if (!loggedAuthFailure) {
                        Log.w(TAG, "ingest rejected the app key (401) — dropping batch, check the configured key")
                        loggedAuthFailure = true
                    }
                }
                is SendResult.TooLarge -> {
                    // Something on the path refused the body size. Retrying it unchanged is
                    // the one failure that never ends: the batch cannot shrink on its own, so
                    // it blocks every line behind it for as long as the process lives. Halve
                    // and retry; a single line that still will not fit is dropped, because one
                    // unshippable line must not cost the whole session.
                    if (batch.size > 1) {
                        batchLineCap = maxOf(1, batch.size / 2)
                        synchronized(lock) {
                            batch.asReversed().forEach { buffer.addFirst(it) }
                        }
                        Log.w(TAG, "ingest refused a ${batch.size}-line batch as too large — retrying at $batchLineCap")
                    } else {
                        Log.w(TAG, "ingest refused a single ${batch[0].length}-byte line — dropping it")
                        synchronized(lock) { droppedSinceMarker++ }
                    }
                }
            }
        }
    }

    /**
     * Persist the unshipped buffer now, ignoring the write interval — call from the host's
     * teardown hooks (service onDestroy, ON_STOP, the uncaught-exception handler). A no-op
     * when the spool is off, already disabled, or nothing new has been logged since the last
     * write, so calling it liberally is cheap.
     */
    suspend fun persistNow() = withContext(Dispatchers.IO) { maybeSpool(force = true) }

    /**
     * Restore a previous process's unshipped lines, once, at startup. Call off the main thread
     * — it does file I/O. The file is deleted *before* its contents are used, so a crash mid
     * replay cannot leave a backlog that grows across boots (the failure mode that killed the
     * previous attempt). Returns the number of lines restored.
     */
    suspend fun replaySpool(): Int = withContext(Dispatchers.IO) {
        val file = spoolFile ?: return@withContext 0
        if (spoolDisabled) return@withContext 0
        val lines = runCatching {
            if (!file.isFile || file.length() == 0L) return@withContext 0
            val text = file.readText()
            // Delete first: from here on the data lives only in memory, so no outcome of the
            // replay can produce a file that survives to the next boot.
            file.delete()
            text.lineSequence().filter { it.isNotBlank() }.toList()
        }.getOrElse {
            disableSpool("replay failed", it)
            return@withContext 0
        }
        if (lines.isEmpty()) return@withContext 0

        // Keep the NEWEST, and cap them: a long offline stretch must not push the session
        // that is about to happen out of a bounded buffer.
        val restored = lines.takeLast(spoolMaxReplayLines)
        synchronized(lock) {
            restored.asReversed().forEach { buffer.addFirst(it) }
            while (buffer.size > maxBufferedLines) {
                buffer.pollFirst()
                droppedSinceMarker++
            }
        }
        // Marker so a replay is visible in the sink rather than looking like duplicate lines.
        enqueue("INFO", TAG, "spool: replayed ${restored.size} unshipped lines from a previous process")
        restored.size
    }

    /**
     * Write the buffer to [spoolFile]. Whole-file rewrite through a temp file + rename, so a
     * kill mid-write leaves either the old file or the new one, never a torn one.
     */
    private fun maybeSpool(force: Boolean) {
        val file = spoolFile ?: return
        if (spoolDisabled) return
        val now = System.currentTimeMillis()
        if (!force && now - lastSpoolWriteMs < spoolMinWriteIntervalMs) return
        // Nothing logged since the last write — the file on disk is still accurate.
        if (seq.get() == lastSpooledSeq) return

        val snapshot = synchronized(lock) { buffer.toList() }
        if (snapshot.isEmpty()) {
            clearSpool()
            return
        }
        // Newest-first accumulation so the cap keeps the most recent lines, then restore order.
        val kept = ArrayDeque<String>()
        var bytes = 0
        for (line in snapshot.asReversed()) {
            val cost = line.length + 1
            if (bytes + cost > spoolMaxBytes) break
            kept.addFirst(line)
            bytes += cost
        }
        if (kept.isEmpty()) return

        runCatching {
            val tmp = File(file.parentFile, file.name + ".tmp")
            tmp.writeText(kept.joinToString("\n"))
            if (!tmp.renameTo(file)) {
                tmp.delete()
                throw IOException("spool rename failed")
            }
        }.onFailure {
            disableSpool("write failed", it)
            return
        }
        lastSpoolWriteMs = now
        lastSpooledSeq = seq.get()
    }

    private fun clearSpool() {
        val file = spoolFile ?: return
        if (spoolDisabled) return
        runCatching { if (file.exists()) file.delete() }
        lastSpooledSeq = seq.get()
    }

    private fun disableSpool(what: String, cause: Throwable) {
        spoolDisabled = true
        Log.w(TAG, "spool $what — disabled for this process, logging continues", cause)
    }

    private sealed class SendResult {
        object Ok : SendResult()
        object Drop : SendResult()

        /** 413 — the body was refused for its size, by the shim or by anything in front of it.
         *  Distinct from [RetryLater] because the same bytes will be refused every time. */
        object TooLarge : SendResult()
        data class RetryLater(val retryAfterMs: Long) : SendResult()
    }

    private fun post(ndjson: String): SendResult {
        return try {
            val conn = (URL(ingestUrl).openConnection() as HttpURLConnection).apply {
                requestMethod = "POST"
                connectTimeout = 10_000
                readTimeout = 10_000
                doOutput = true
                authorize()
                setRequestProperty("Content-Type", "application/x-ndjson")
            }
            conn.outputStream.use { it.write(ndjson.toByteArray(Charsets.UTF_8)) }
            val code = conn.responseCode
            val retryAfterMs = (conn.getHeaderField("Retry-After")?.toLongOrNull() ?: 0L) * 1000L
            conn.disconnect()
            when {
                code in 200..299 -> SendResult.Ok
                code == 401 -> SendResult.Drop
                // Access refused the request (no or a wrong service token): the same bytes
                // are refused every time, so this is a 401, not a retry.
                code in 300..399 -> SendResult.Drop
                code == 429 -> SendResult.RetryLater(retryAfterMs)
                code == 413 -> SendResult.TooLarge
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
                authorize()
            }
            if (conn.responseCode in 200..299) {
                val body = conn.inputStream.use { it.readBytes().toString(Charsets.UTF_8) }
                val level = JSONObject(body).optString("level", "").uppercase()
                LEVELS[level]?.let { minLevel = it }
                configKnown = true
            }
            conn.disconnect()
        } catch (_: IOException) {
            // Keep the current level; next refresh retries.
        }
    }
}
