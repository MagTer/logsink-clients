package se.falle.logsink

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import java.util.concurrent.CopyOnWriteArrayList

/**
 * The Cloudflare Access leg (home-server APPLOGS-MIGRATION-DESIGN A4): the service token is
 * sent on every request when configured and never otherwise, and a refused request (Access
 * answers 302 to its login page) is neither followed nor mistaken for a shipped batch.
 */
class LogsinkClientAccessTest {
    private lateinit var server: MockWebServer
    private val seen = CopyOnWriteArrayList<RecordedRequest>()
    private var ingestStatus = 204
    private val scope = CoroutineScope(Job())

    @Before
    fun start() {
        server = MockWebServer()
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                seen += request
                return when (request.path) {
                    "/ingest/config" -> MockResponse().setBody("""{"app":"t","level":"DEBUG"}""")
                    "/ingest" -> if (ingestStatus in 300..399)
                        MockResponse().setResponseCode(ingestStatus).setHeader("Location", "/login")
                    else MockResponse().setResponseCode(ingestStatus)
                    // What a followed redirect would have reached: a 200 that ships nothing.
                    else -> MockResponse().setBody("<html>sign in</html>")
                }
            }
        }
        server.start()
    }

    @After
    fun stop() {
        scope.cancel()
        server.shutdown()
    }

    private fun client(id: String? = null, secret: String? = null) = LogsinkClient(
        ingestUrl = server.url("/ingest").toString(),
        apiKey = "k",
        accessClientId = id,
        accessClientSecret = secret,
        flushIntervalMs = 3_600_000L,
        scope = scope,
    )

    private fun posts() = seen.filter { it.method == "POST" && it.path == "/ingest" }

    private fun shipOneLine(c: LogsinkClient) = runBlocking {
        c.enqueue("WARN", "t", "hello")
        c.flushNow()
    }

    @Test
    fun `headers go on the POST and on the config GET when both are set`() {
        val c = client("id.access", "s3cret")
        shipOneLine(c)
        val post = posts().single()
        assertEquals("id.access", post.getHeader("CF-Access-Client-Id"))
        assertEquals("s3cret", post.getHeader("CF-Access-Client-Secret"))
        assertEquals("Bearer k", post.getHeader("Authorization"))
        val config = seen.first { it.path == "/ingest/config" }
        assertEquals("id.access", config.getHeader("CF-Access-Client-Id"))
        assertEquals("s3cret", config.getHeader("CF-Access-Client-Secret"))
    }

    @Test
    fun `no Access headers when none are configured`() {
        shipOneLine(client())
        assertTrue(seen.isNotEmpty())
        seen.forEach {
            assertNull(it.getHeader("CF-Access-Client-Id"))
            assertNull(it.getHeader("CF-Access-Client-Secret"))
        }
    }

    @Test
    fun `half a token is no token`() {
        shipOneLine(client("id.access", " "))
        seen.forEach { assertNull(it.getHeader("CF-Access-Client-Id")) }
    }

    @Test
    fun `a refused request is not followed to the login page`() {
        ingestStatus = 302
        shipOneLine(client("id.access", "wrong"))
        assertEquals(1, posts().size)
        assertTrue(seen.none { it.path == "/login" })
    }

    @Test
    fun `a refused batch is dropped like a 401, not retried forever`() {
        ingestStatus = 302
        val c = client("id.access", "wrong")
        shipOneLine(c)
        runBlocking { c.flushNow() }
        assertEquals(1, posts().size)
    }
}
