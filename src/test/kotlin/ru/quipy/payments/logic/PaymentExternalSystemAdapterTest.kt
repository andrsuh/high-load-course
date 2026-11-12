package ru.quipy.payments.logic

import com.sun.net.httpserver.HttpServer
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import ru.quipy.apigateway.ratelimit.PaymentCallAdmission
import ru.quipy.payments.api.PaymentProcessedEvent
import ru.quipy.payments.api.PaymentSubmittedEvent
import java.net.InetSocketAddress
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class PaymentExternalSystemAdapterTest {
    @Test
    fun `temporary business failure is retried with the same payment and a new transaction`() = Fixture().use { f ->
        f.responses.add(Reply(result = false, message = "Temporary error", delayMs = 100))
        f.responses.add(Reply())
        f.pay()
        assertEquals(2, f.requests.size)
        assertEquals(listOf(f.paymentId.toString(), f.paymentId.toString()), f.requests.map { it.getValue("paymentId") })
        assertEquals(2, f.requests.map { it.getValue("transactionId") }.distinct().size)
        assertEquals(listOf(false, true), f.processings.map { it.success })
        assertEquals(2, f.permits)
        assertTrue(Duration.parse(f.requests[1].getValue("timeout")) < Duration.parse(f.requests[0].getValue("timeout")))
        assertEquals(f.submissions.map { it.transactionId }, f.processings.map { it.transactionId })
    }

    @Test
    fun `success is never repeated`() = Fixture().use { f ->
        f.pay()
        assertEquals(1, f.requests.size)
        assertEquals(listOf(true), f.processings.map { it.success })
    }

    @ParameterizedTest
    @ValueSource(ints = [400, 401, 403, 404, 405])
    fun `permanent HTTP errors are not retried even if the body says success`(status: Int) = Fixture().use { f ->
        f.responses.add(Reply(status = status))
        f.pay()
        assertEquals(1, f.requests.size)
        assertEquals(listOf(false), f.processings.map { it.success })
    }

    @ParameterizedTest
    @ValueSource(ints = [429, 500, 502, 503, 504])
    fun `temporary HTTP errors are explicitly retried and logged even with invalid JSON`(status: Int) = Fixture().use { f ->
        f.responses.add(Reply(status = status, rawBody = "unavailable", retryAfter = "0"))
        f.pay()
        assertEquals(2, f.requests.size)
        assertEquals(2, f.permits)
        assertEquals(2, f.submissions.size)
        assertEquals(listOf(false, true), f.processings.map { it.success })
    }

    @Test
    fun `permanent business failure is not retried`() = Fixture().use { f ->
        f.responses.add(Reply(result = false, message = "Payment declined"))
        f.pay()
        assertEquals(1, f.requests.size)
        assertFalse(f.processings.single().success)
    }

    @Test
    fun `invalid success body is not blindly retried`() = Fixture().use { f ->
        f.responses.add(Reply(rawBody = "not JSON"))
        f.pay()
        assertEquals(1, f.requests.size)
        assertEquals("Invalid payment provider response", f.processings.single().reason)
    }

    @Test
    fun `attempt count is bounded`() = Fixture(maxAttempts = 3).use { f ->
        repeat(5) { f.responses.add(Reply(result = false, message = "Temporary error")) }
        f.pay()
        assertEquals(3, f.requests.size)
        assertEquals(3, f.processings.size)
        assertTrue(f.processings.none { it.success })
    }

    @Test
    fun `retry is not started with less than the account average processing time remaining`() =
        Fixture(averageProcessingTime = Duration.ofMillis(700)).use { f ->
            f.responses.add(Reply(result = false, message = "Temporary error", delayMs = 400))
            f.pay(budgetMs = 1_000)
            assertEquals(1, f.requests.size)
            assertEquals(listOf(false, false), f.processings.map { it.success })
            assertTrue(f.processings.last().reason!!.contains("Deadline"))
        }

    @Test
    fun `retry after seconds is observed within the original deadline`() = Fixture().use { f ->
        f.responses.add(Reply(status = 429, retryAfter = "1"))
        val startedAt = System.nanoTime()
        f.pay()
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt) >= 1_000)
        assertEquals(listOf(false, true), f.processings.map { it.success })
    }

    @Test
    fun `retry after that exceeds the remaining budget causes no further request`() = Fixture().use { f ->
        f.responses.add(Reply(status = 429, retryAfter = "10"))
        f.pay(budgetMs = 500)
        assertEquals(1, f.requests.size)
        assertTrue(f.processings.last().reason!!.contains("Deadline"))
    }

    @Test
    fun `retry after HTTP date is respected`() = Fixture().use { f ->
        val date = ZonedDateTime.now(java.time.ZoneOffset.UTC).plusSeconds(10).format(DateTimeFormatter.RFC_1123_DATE_TIME)
        f.responses.add(Reply(status = 503, retryAfter = date))
        f.pay(budgetMs = 500)
        assertEquals(1, f.requests.size)
        assertTrue(f.processings.last().reason!!.contains("Deadline"))
    }

    @Test
    fun `expired deadline sends no request`() = Fixture().use { f ->
        f.pay(budgetMs = -1)
        assertEquals(0, f.requests.size)
        assertEquals(0, f.permits)
        assertFalse(f.processings.single().success)
    }

    @Test
    fun `denied retry permit sends no further request`() = Fixture().use { f ->
        f.responses.add(Reply(result = false, message = "Temporary error"))
        f.allowPermit = { it == 1 }
        f.pay()
        assertEquals(1, f.requests.size)
        assertEquals(2, f.permits)
        assertTrue(f.processings.last().reason!!.contains("rate permit"))
    }

    @Test
    fun `time spent waiting for a permit does not extend the deadline`() = Fixture().use { f ->
        f.allowPermit = { Thread.sleep(150); true }
        val startedAt = System.nanoTime()
        f.pay(budgetMs = 100)
        assertEquals(0, f.requests.size)
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt) < 500)
        assertFalse(f.processings.last().success)
    }

    @Test
    fun `slow response is bounded by client deadline and its unknown outcome is not retried`() = Fixture().use { f ->
        f.responses.add(Reply(delayMs = 2_000))
        val startedAt = System.nanoTime()
        f.pay(budgetMs = 250)
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt) < 1_000)
        assertEquals(1, f.requests.size)
        assertTrue(f.processings.single().reason!!.startsWith("Unknown payment outcome"))
    }

    @Test
    fun `failure to persist a successful payment does not cause another charge`() = Fixture().use { f ->
        f.failProcessingWrite = true
        assertThrows(IllegalStateException::class.java) { f.pay() }
        assertEquals(1, f.requests.size)
    }

    private data class Reply(
        val status: Int = 200,
        val result: Boolean = true,
        val message: String? = null,
        val retryAfter: String? = null,
        val rawBody: String? = null,
        val delayMs: Long = 0,
    )

    private class Fixture(maxAttempts: Int = 5, averageProcessingTime: Duration = Duration.ofMillis(10)) : AutoCloseable {
        val paymentId: UUID = UUID.randomUUID()
        val submissions = mutableListOf<PaymentSubmittedEvent>()
        val processings = mutableListOf<PaymentProcessedEvent>()
        val requests = CopyOnWriteArrayList<Map<String, String>>()
        val responses = java.util.concurrent.LinkedBlockingQueue<Reply>()
        var permits = 0
        var allowPermit: (Int) -> Boolean = { true }
        var failProcessingWrite = false
        private val state = PaymentAggregateState().also {
            it.paymentCreatedApply(it.create(paymentId, UUID.randomUUID(), 100))
        }
        private val executor = Executors.newCachedThreadPool()
        private val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            this.executor = this@Fixture.executor
            createContext("/external/process") { exchange ->
                try {
                    requests.add(exchange.requestURI.rawQuery.split('&').associate { pair ->
                        val parts = pair.split('=', limit = 2)
                        URLDecoder.decode(parts[0], StandardCharsets.UTF_8) to URLDecoder.decode(parts[1], StandardCharsets.UTF_8)
                    })
                    val reply = responses.poll() ?: Reply()
                    if (reply.delayMs > 0) Thread.sleep(reply.delayMs)
                    reply.retryAfter?.let { exchange.responseHeaders.add("Retry-After", it) }
                    val body = (reply.rawBody ?: """{"transactionId":"tx","paymentId":"$paymentId","result":${reply.result},"message":${reply.message?.let { "\"$it\"" } ?: "null"}}""").toByteArray()
                    exchange.sendResponseHeaders(reply.status, body.size.toLong())
                    exchange.responseBody.use { it.write(body) }
                } catch (_: java.io.IOException) {
                    // The deadline test deliberately closes the client connection first.
                } catch (_: InterruptedException) {
                    Thread.currentThread().interrupt()
                } finally {
                    exchange.close()
                }
            }
            start()
        }

        private val recorder = object : PaymentAttemptRecorder {
            override fun submitted(paymentId: UUID, transactionId: UUID, startedAt: Long, spentInQueue: Duration) {
                val event = state.logSubmission(true, transactionId, startedAt, spentInQueue)
                submissions.add(event)
                state.paymentSubmittedApply(event)
            }

            override fun processed(paymentId: UUID, transactionId: UUID, success: Boolean, processedAt: Long, reason: String?) {
                if (failProcessingWrite) error("Event store unavailable")
                val event = state.logProcessing(success, processedAt, transactionId, reason)
                processings.add(event)
                state.paymentSubmittedApply(event)
            }
        }

        private val adapter = PaymentExternalSystemAdapterImpl(
            PaymentAccountProperties("service", "acc-8", 50, 10, 30, averageProcessingTime, true),
            recorder,
            "127.0.0.1:${server.address.port}",
            "test-token",
            object : PaymentCallAdmission {
                override fun awaitCall(accountName: String, deadline: Long): Boolean = allowPermit(++permits)
            },
            maxAttempts,
        )

        fun pay(budgetMs: Long = 3_500) {
            val startedAt = now()
            adapter.performPaymentAsync(paymentId, 100, startedAt, startedAt + budgetMs)
        }

        override fun close() {
            server.stop(0)
            executor.shutdownNow()
        }
    }
}
