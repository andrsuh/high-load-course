package ru.quipy.apigateway.ratelimit

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.http.HttpHeaders
import org.springframework.http.ResponseEntity
import org.springframework.web.context.request.async.DeferredResult
import ru.quipy.payments.logic.PaymentAccountProperties
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit

class PayOrderAdmissionQueueTest {
    @Test
    fun `calls reserve the account average processing time`() {
        val queue = queue(account(parallelRequests = 50, rateLimitPerSec = 10, processingTime = Duration.ofMillis(700)))
        try {
            assertFalse(queue.awaitCall("acc-5", System.currentTimeMillis() + 500))
            assertTrue(queue.awaitCall("acc-5", System.currentTimeMillis() + 1_000))
        } finally {
            queue.close()
        }
    }

    @Test
    fun `request waits without taking an extra parallel slot`() {
        val queue = queue(account(parallelRequests = 1, rateLimitPerSec = 10, processingTime = Duration.ofMillis(100)))
        val releases = CopyOnWriteArrayList<() -> Unit>()
        try {
            val deadline = System.currentTimeMillis() + 5_000
            val first = queue.submit(deadline) { onFinished ->
                releases.add(onFinished)
                "first"
            }
            assertEquals("first", awaitResult(first))

            val second = queue.submit(deadline) { onFinished ->
                onFinished()
                "second"
            }
            assertFalse(second.hasResult())

            releases.single().invoke()
            assertEquals("second", awaitResult(second))
        } finally {
            releases.forEach { it.invoke() }
            queue.close()
        }
    }

    @Test
    fun `queue capacity follows account throughput and remaining deadline`() {
        val queue = queue(account(parallelRequests = 5, rateLimitPerSec = 10, processingTime = Duration.ofMillis(4_900)))
        val releases = CopyOnWriteArrayList<() -> Unit>()
        try {
            val deadline = System.currentTimeMillis() + 60_000
            val active = (1..5).map { index ->
                queue.submit(deadline) { onFinished ->
                    releases.add(onFinished)
                    index
                }
            }
            active.forEach { awaitResult(it) }

            val waiting = (1..45).map {
                queue.submit(deadline) { onFinished ->
                    onFinished()
                    "waiting"
                }
            }
            assertTrue(waiting.none { it.hasResult() })

            val rejected = queue.submit(deadline) { onFinished ->
                onFinished()
                "overflow"
            }
            val response = awaitResult(rejected) as ResponseEntity<*>
            assertEquals(429, response.statusCode.value())
            assertTrue(response.headers.getFirst(HttpHeaders.RETRY_AFTER)!!.toLong() > 10)
        } finally {
            releases.forEach { it.invoke() }
            queue.close()
        }
    }

    @Test
    fun `rejected requests receive separate retry slots`() {
        val queue = queue(
            account(parallelRequests = 1, rateLimitPerSec = 1, processingTime = Duration.ofSeconds(1)),
            maxQueueSize = 1,
            retryAfter = Duration.ofSeconds(1),
        )
        val releases = CopyOnWriteArrayList<() -> Unit>()
        try {
            val deadline = System.currentTimeMillis() + 10_000
            val active = queue.submit(deadline) { onFinished ->
                releases.add(onFinished)
                "active"
            }
            assertEquals("active", awaitResult(active))

            val waiting = queue.submit(deadline) { onFinished ->
                onFinished()
                "waiting"
            }
            assertFalse(waiting.hasResult())

            val firstRejected = queue.submit(deadline) { onFinished ->
                onFinished()
                "first rejected"
            }
            val secondRejected = queue.submit(deadline) { onFinished ->
                onFinished()
                "second rejected"
            }
            assertEquals("1", (awaitResult(firstRejected) as ResponseEntity<*>).headers.getFirst(HttpHeaders.RETRY_AFTER))
            assertEquals("2", (awaitResult(secondRejected) as ResponseEntity<*>).headers.getFirst(HttpHeaders.RETRY_AFTER))
        } finally {
            releases.forEach { it.invoke() }
            queue.close()
        }
    }

    @Test
    fun `request is rejected when provider cannot finish before deadline`() {
        val queue = queue(account(parallelRequests = 5, rateLimitPerSec = 10, processingTime = Duration.ofMillis(4_900)))
        try {
            val result = queue.submit(System.currentTimeMillis() + 10_000) { onFinished ->
                onFinished()
                "too late"
            }

            assertEquals(429, (awaitResult(result) as ResponseEntity<*>).statusCode.value())
        } finally {
            queue.close()
        }
    }

    @Test
    fun `relative safety margin admits a three second deadline`() {
        val queue = queue(account(parallelRequests = 16, rateLimitPerSec = 110, processingTime = Duration.ofSeconds(1)))
        try {
            val result = queue.submit(System.currentTimeMillis() + 3_000) { onFinished ->
                onFinished()
                "accepted"
            }

            assertEquals("accepted", awaitResult(result))
        } finally {
            queue.close()
        }
    }

    @Test
    fun `actual calls to the same account keep their rolling rate limit`() {
        val firstAccount = account(parallelRequests = 2, rateLimitPerSec = 1, processingTime = Duration.ofMillis(100))
        val queue = queue(firstAccount, firstAccount.copy(accountName = "acc-6"))
        try {
            val deadline = System.currentTimeMillis() + 3_000
            val startedAt = System.nanoTime()
            assertTrue(queue.awaitCall("acc-6", deadline))
            assertTrue(queue.awaitCall("acc-6", deadline))
            assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt) >= 900)
        } finally {
            queue.close()
        }
    }

    private fun account(parallelRequests: Int, rateLimitPerSec: Int, processingTime: Duration) =
        PaymentAccountProperties("service", "acc-5", parallelRequests, rateLimitPerSec, 1, processingTime, true)

    private fun queue(
        vararg accounts: PaymentAccountProperties,
        maxQueueSize: Int = 100,
        retryAfter: Duration = Duration.ofSeconds(10),
    ): PayOrderAdmissionQueue {
        val settings = PayOrderAdmissionProperties().apply {
            this.maxQueueSize = maxQueueSize
            this.retryAfter = retryAfter
        }
        return PayOrderAdmissionQueue(accounts.toList(), settings)
    }

    private fun awaitResult(result: DeferredResult<*>): Any? {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2)
        while (!result.hasResult() && System.nanoTime() < deadline) Thread.sleep(5)
        assertTrue(result.hasResult(), "Deferred result did not complete")
        return result.result
    }
}
