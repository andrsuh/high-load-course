package ru.quipy.payments.logic

import com.sun.net.httpserver.HttpServer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.InetSocketAddress
import java.time.Duration
import java.util.UUID

class PaymentHttpTimeoutTest {
    @Suppress("UNCHECKED_CAST")
    @Test
    fun `HTTP call respects remaining deadline and records timeout`() {
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/external/process") { exchange ->
            Thread.sleep(3000)
            exchange.close()
        }
        server.start()
        try {
            val registry = SimpleMeterRegistry()
            val eventStore = mock(EventSourcingService::class.java, org.mockito.stubbing.Answer { null }) as EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
            val adapter = PaymentExternalSystemAdapterImpl(
                PaymentAccountProperties("test", "acc-test", 1, 10, 1, Duration.ofSeconds(1), true),
                eventStore, "127.0.0.1:${server.address.port}", "test-token", registry, 4000
            )
            val started = now()
            val result = adapter.performPaymentAsync(UUID.randomUUID(), 1, started, started + 1000)
            assertFalse(result.success)
            assertTrue(result.retryable)
            assertTrue(now() - started < 2500, "Call must use deadline instead of 4 second configured timeout")
            assertEquals(1L, registry.get("payment_http_duration").tag("status", "timeout").timer().count())
        } finally {
            server.stop(0)
        }
    }
}
