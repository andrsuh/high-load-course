package ru.quipy.payments.logic

import com.sun.net.httpserver.HttpServer
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import ru.quipy.core.EventSourcingService
import ru.quipy.domain.Event
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.api.PaymentCreatedEvent
import ru.quipy.payments.api.PaymentProcessedEvent
import ru.quipy.payments.api.PaymentSubmittedEvent
import java.net.InetSocketAddress
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class PaymentExternalSystemAdapterTest {
    @Test
    fun `short payment retries a temporary failure and reports only final success`() {
        Fixture { attempt -> if (attempt == 1) "Temporary error" else null }.use { fixture ->
            val adapter = fixture.adapter()
            val started = now()
            adapter.performPaymentAsync(fixture.paymentId, 100, started, started + 2_900)

            val result = fixture.awaitResult()
            assertTrue(result.success)
            assertEquals(2, fixture.requests.get())
            assertEquals(2, fixture.submissions.size)
            assertEquals(2, fixture.submissions.map { it.transactionId }.distinct().size)
            assertEquals(1, fixture.results.size)
        }
    }

    @Test
    fun `burst payment stops retrying after three temporary failures`() {
        Fixture { "Temporary error" }.use { fixture ->
            val adapter = fixture.adapter()
            val started = now()
            adapter.performPaymentAsync(fixture.paymentId, 100, started, started + 30_000)

            val result = fixture.awaitResult()
            assertFalse(result.success)
            assertEquals("Temporary error", result.reason)
            assertEquals(3, fixture.requests.get())
            assertEquals(3, fixture.submissions.size)
            assertEquals(1, fixture.results.size)
        }
    }

    @Test
    fun `provider timeout retries and exports metrics used by the case seven dashboard`() {
        Fixture { attempt -> if (attempt == 1) "Timeout" else null }.use { fixture ->
            val adapter = fixture.adapter()
            val started = now()
            adapter.performPaymentAsync(fixture.paymentId, 100, started, started + 2_900)

            assertTrue(fixture.awaitResult().success)
            assertEquals(2, fixture.requests.get())
            assertTrue(fixture.requestParameters.all { it["timeout"] == "PT0.2S" })
            await().atMost(Duration.ofSeconds(2)).untilAsserted {
                val scrape = fixture.registry.scrape()
                val labels = "accountName=\"acc-test\",service=\"test-service\""
                assertTrue(scrape.contains("payment_external_retries_total{$labels} 1.0"), scrape)
                assertTrue(scrape.contains("payment_external_timeouts_total{$labels} 1.0"), scrape)
                assertTrue(scrape.contains("payment_external_request_duration_milliseconds_count{$labels} 2"), scrape)
                assertTrue(scrape.contains("payment_external_request_duration_milliseconds_sum{$labels}"), scrape)
                assertTrue(scrape.contains("payment_external_request_duration_milliseconds_bucket{"), scrape)
            }
        }
    }

    @Test
    fun `expired payment fails without calling the provider`() {
        Fixture { null }.use { fixture ->
            val adapter = fixture.adapter()
            val started = now()
            adapter.performPaymentAsync(fixture.paymentId, 100, started, started - 1)

            val result = fixture.awaitResult()
            assertFalse(result.success)
            assertEquals("Deadline exceeded before submit", result.reason)
            assertEquals(0, fixture.requests.get())
        }
    }

    @Test
    fun `short admission bounds reservations and permits reuse after release`() {
        Fixture { null }.use { fixture ->
            val adapter = fixture.adapter(rate = 2, parallel = 1)
            val deadline = now() + 2_900
            val first = UUID.randomUUID()
            assertNull(adapter.tryReserve(first, deadline))
            assertNull(adapter.tryReserve(UUID.randomUUID(), deadline))
            assertNotNull(adapter.tryReserve(UUID.randomUUID(), deadline))
            adapter.releaseReservation(first)
            assertNull(adapter.tryReserve(UUID.randomUUID(), deadline))
        }
    }

    @Test
    fun `burst admission buffers beyond instantaneous capacity but stays bounded`() {
        Fixture { null }.use { fixture ->
            val adapter = fixture.adapter(rate = 2, parallel = 1)
            val deadline = now() + 30_000
            repeat(40) { assertNull(adapter.tryReserve(UUID.randomUUID(), deadline)) }
            assertNotNull(adapter.tryReserve(UUID.randomUUID(), deadline))
        }
    }

    private class Fixture(private val responseMessage: (Int) -> String?) : AutoCloseable {
        val paymentId = UUID.randomUUID()
        val requests = AtomicInteger()
        val requestParameters = CopyOnWriteArrayList<Map<String, String>>()
        val submissions = CopyOnWriteArrayList<PaymentSubmittedEvent>()
        val results = CopyOnWriteArrayList<PaymentProcessedEvent>()
        private val completed = CountDownLatch(1)
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        private val state = PaymentAggregateState().apply {
            paymentCreatedApply(PaymentCreatedEvent(paymentId, UUID.randomUUID(), 100))
        }
        private val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/external/process") { exchange ->
                val parameters = exchange.requestURI.rawQuery.split('&').associate {
                    val pair = it.split('=', limit = 2)
                    pair[0] to URLDecoder.decode(pair[1], StandardCharsets.UTF_8)
                }
                requestParameters.add(parameters)
                val message = responseMessage(requests.incrementAndGet())
                val response = PaymentExternalSystemAdapterImpl.mapper.writeValueAsBytes(
                    ExternalSysResponse(
                        parameters.getValue("transactionId"), parameters.getValue("paymentId"),
                        message == null, message,
                    )
                )
                exchange.requestBody.close()
                exchange.responseHeaders.add("Content-Type", "application/json")
                exchange.sendResponseHeaders(200, response.size.toLong())
                exchange.responseBody.use { it.write(response) }
            }
            start()
        }

        @Suppress("UNCHECKED_CAST")
        private val eventService = Mockito.mock(EventSourcingService::class.java) { invocation ->
            if (invocation.method.name != "update") {
                throw AssertionError("Unexpected event store call: ${invocation.method.name}")
            }
            synchronized(state) {
                val command = invocation.arguments[2] as (PaymentAggregateState) -> Event<PaymentAggregate>
                val event = command(state)
                when (event) {
                    is PaymentSubmittedEvent -> {
                        state.paymentSubmittedApply(event)
                        submissions.add(event)
                    }
                    is PaymentProcessedEvent -> {
                        state.paymentSubmittedApply(event)
                        results.add(event)
                        completed.countDown()
                    }
                    else -> throw AssertionError("Unexpected event: $event")
                }
                event
            }
        } as EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

        fun adapter(rate: Int = 10, parallel: Int = 1) = PaymentExternalSystemAdapterImpl(
            PaymentAccountProperties("test-service", "acc-test", parallel, rate, 1, Duration.ofMillis(100), true),
            eventService, "127.0.0.1:${server.address.port}", "test-token", registry,
        )

        fun awaitResult(): PaymentProcessedEvent {
            assertTrue(completed.await(5, TimeUnit.SECONDS), "Payment did not finish")
            return results.single()
        }

        override fun close() {
            server.stop(0)
            registry.close()
        }
    }
}
