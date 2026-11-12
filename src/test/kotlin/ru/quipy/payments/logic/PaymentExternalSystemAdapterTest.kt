package ru.quipy.payments.logic

import com.sun.net.httpserver.HttpServer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito
import okhttp3.HttpUrl.Companion.toHttpUrl
import ru.quipy.apigateway.ratelimit.PaymentCallAdmission
import ru.quipy.core.BasicAggregateRegistry
import ru.quipy.core.EventSourcingProperties
import ru.quipy.core.EventSourcingService
import ru.quipy.database.EventStore
import ru.quipy.domain.EventRecord
import ru.quipy.mapper.JsonEventMapper
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.api.PaymentCreatedEvent
import ru.quipy.payments.api.PaymentProcessedEvent
import ru.quipy.payments.api.PaymentSubmittedEvent
import java.net.InetSocketAddress
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
    fun `HTTP retry statuses follow the supplied configuration`() = Fixture(retryableStatuses = setOf(409)).use { f ->
        f.responses.add(Reply(status = 409))
        f.responses.add(Reply(status = 500))
        f.pay()
        assertEquals(2, f.requests.size)
        assertEquals(listOf(false, false), f.processings.map { it.success })
        assertTrue(f.processings.last().reason!!.startsWith("HTTP 500"))
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
    fun `retry can finish below the average when its scaled estimate fits`() =
        Fixture(averageProcessingTime = Duration.ofMillis(700), processingTimeFactor = 0.7).use { f ->
            f.responses.add(Reply(result = false, message = "Temporary error", delayMs = 400))
            f.pay(budgetMs = 1_000)
            assertEquals(2, f.requests.size)
            assertEquals(listOf(false, true), f.processings.map { it.success })
            assertEquals(listOf(Duration.ofMillis(700), Duration.ofMillis(490)), f.processingTimeEstimates)
        }

    @Test
    fun `initial attempt still requires the full account average`() =
        Fixture(averageProcessingTime = Duration.ofMillis(700), processingTimeFactor = 0.7).use { f ->
            f.pay(budgetMs = 600)
            assertTrue(f.requests.isEmpty())
            assertEquals(0, f.permits)
            assertTrue(f.processings.single().reason!!.contains("Deadline"))
        }

    @Test
    fun `retry estimate scales with the selected account`() =
        Fixture(averageProcessingTime = Duration.ofMillis(200), processingTimeFactor = 0.7).use { f ->
            f.responses.add(Reply(result = false, message = "Temporary error"))
            f.pay()
            assertEquals(listOf(Duration.ofMillis(200), Duration.ofMillis(140)), f.processingTimeEstimates)
            assertTrue(f.processings.last().success)
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

    private class Fixture(
        maxAttempts: Int = 5,
        averageProcessingTime: Duration = Duration.ofMillis(10),
        retryableStatuses: Set<Int> = setOf(429, 500, 502, 503, 504),
        processingTimeFactor: Double = 1.0,
    ) : AutoCloseable {
        val paymentId: UUID = UUID.randomUUID()
        val submissions get() = records.filter { it.eventTitle == "PAYMENT_SUBMITTED_EVENT" }
            .map { mapper.toEvent(it.payload, PaymentSubmittedEvent::class) as PaymentSubmittedEvent }
        val processings get() = records.filter { it.eventTitle == "PAYMENT_PROCESSED_EVENT" }
            .map { mapper.toEvent(it.payload, PaymentProcessedEvent::class) as PaymentProcessedEvent }
        val requests = CopyOnWriteArrayList<Map<String, String>>()
        val responses = java.util.concurrent.LinkedBlockingQueue<Reply>()
        var permits = 0
        val processingTimeEstimates = mutableListOf<Duration?>()
        var allowPermit: (Int) -> Boolean = { true }
        var failProcessingWrite = false
        private val executor = Executors.newCachedThreadPool()
        private val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            this.executor = this@Fixture.executor
            createContext("/external/process") { exchange ->
                try {
                    val url = "http://localhost${exchange.requestURI}".toHttpUrl()
                    requests.add(url.queryParameterNames.associateWith { url.queryParameter(it).orEmpty() })
                    val reply = responses.poll() ?: Reply()
                    if (reply.delayMs > 0) Thread.sleep(reply.delayMs)
                    reply.retryAfter?.let { exchange.responseHeaders.add("Retry-After", it) }
                    val body = (reply.rawBody ?: """{"transactionId":"tx","paymentId":"$paymentId","result":${reply.result},"message":${reply.message?.let { "\"$it\"" } ?: "null"}}""").toByteArray()
                    exchange.sendResponseHeaders(reply.status, body.size.toLong())
                    exchange.responseBody.use { it.write(body) }
                } catch (_: java.io.IOException) {
                } catch (_: InterruptedException) {
                    Thread.currentThread().interrupt()
                } finally {
                    exchange.close()
                }
            }
            start()
        }

        private val registry = BasicAggregateRegistry().apply {
            register<UUID, PaymentAggregate, PaymentAggregateState>(PaymentAggregate::class, PaymentAggregateState::class) {
                registerStateTransition(PaymentCreatedEvent::class, PaymentAggregateState::paymentCreatedApply)
                registerStateTransition(PaymentSubmittedEvent::class, PaymentAggregateState::paymentSubmittedApply)
                registerStateTransition(PaymentProcessedEvent::class, PaymentAggregateState::paymentSubmittedApply)
            }
        }
        private val mapper = JsonEventMapper(ObjectMapper().registerKotlinModule().registerModule(JavaTimeModule()))
        private val records = mutableListOf<EventRecord>()
        private val store = Mockito.mock(EventStore::class.java) { invocation ->
            when (invocation.method.name) {
                "findEventRecordsWithAggregateVersionGraterThan" ->
                    records.filter { it.aggregateVersion > invocation.getArgument<Long>(2) }
                "insertEventRecord" -> {
                    val record = invocation.getArgument<EventRecord>(1)
                    if (failProcessingWrite && record.eventTitle == "PAYMENT_PROCESSED_EVENT") error("Event store unavailable")
                    records.add(record)
                    null
                }
                else -> Mockito.RETURNS_DEFAULTS.answer(invocation)
            }
        }
        private val payments = EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>(
            PaymentAggregate::class, registry, mapper,
            Mockito.mock(EventSourcingProperties::class.java).apply {
                Mockito.`when`(snapshotFrequency).thenReturn(100)
                Mockito.`when`(spinLockMaxAttempts).thenReturn(3)
            }, store,
        ).also { service -> service.create { it.create(paymentId, UUID.randomUUID(), 100) } }

        private val adapter = PaymentExternalSystemAdapterImpl(
            PaymentAccountProperties("service", "acc-8", 50, 10, 30, averageProcessingTime, true),
            payments,
            "127.0.0.1:${server.address.port}",
            "test-token",
            object : PaymentCallAdmission {
                override fun awaitCall(accountName: String, deadline: Long, processingTimeEstimate: Duration?): Boolean {
                    processingTimeEstimates.add(processingTimeEstimate)
                    return allowPermit(++permits)
                }
            },
            maxAttempts,
            retryableStatuses,
            processingTimeFactor,
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
