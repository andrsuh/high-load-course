package ru.quipy.payments.logic

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class PaymentRetryTest {
    private class Account(private val outcomes: List<PaymentAttempt>) : PaymentExternalSystemAdapter {
        var calls = 0
        override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) =
            outcomes[minOf(calls++, outcomes.lastIndex)]
        override fun name() = "test"
        override fun price() = 1
        override fun isEnabled() = true
    }

    @Test
    fun `temporary failure is retried and counted once`() {
        val account = Account(listOf(PaymentAttempt(false, true), PaymentAttempt(true)))
        val registry = SimpleMeterRegistry()
        PaymentSystemImpl(listOf(account), registry).submitPaymentRequest(UUID.randomUUID(), 1, now(), now() + 1000)
        assertEquals(2, account.calls)
        assertEquals(1.0, registry.get("payment_retries").counter().count())
        assertEquals(1.0, registry.get("http_request_success_pay").counter().count())
    }

    @Test
    fun `permanent failure is not retried`() {
        val account = Account(listOf(PaymentAttempt(false)))
        PaymentSystemImpl(listOf(account), SimpleMeterRegistry()).submitPaymentRequest(UUID.randomUUID(), 1, now(), now() + 1000)
        assertEquals(1, account.calls)
    }

    @Test
    fun `expired request never calls provider`() {
        val account = Account(listOf(PaymentAttempt(true)))
        PaymentSystemImpl(listOf(account), SimpleMeterRegistry()).submitPaymentRequest(UUID.randomUUID(), 1, now(), now() - 1)
        assertEquals(0, account.calls)
    }
}
