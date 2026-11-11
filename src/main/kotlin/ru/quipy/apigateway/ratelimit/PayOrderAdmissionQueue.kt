package ru.quipy.apigateway.ratelimit

import jakarta.annotation.PreDestroy
import io.micrometer.core.instrument.Metrics
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Configuration
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Component
import org.springframework.web.context.request.async.DeferredResult
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.payments.logic.OrderPayer
import ru.quipy.payments.logic.PaymentAccountProperties
import java.time.Duration
import java.util.ArrayDeque
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.math.ceil

@ConfigurationProperties(prefix = "api.pay-order-admission")
class PayOrderAdmissionProperties {
    var maxQueueSize: Int = 2_000
    var safetyMarginFactor: Double = 0.1
    var processingTimeReserveFactor: Double = 2.0
    var retryAfter: Duration = Duration.ofSeconds(1)
}

@Configuration
@EnableConfigurationProperties(PayOrderAdmissionProperties::class)
class PayOrderAdmissionConfiguration

interface PayOrderRequestQueue {
    fun <T : Any> submit(deadline: Long, action: (onFinished: () -> Unit) -> T): DeferredResult<T>
}

interface PaymentCallAdmission {
    fun awaitCall(accountName: String, deadline: Long): Boolean
}

@Component
class PayOrderAdmissionQueue(
    paymentAccounts: List<PaymentAccountProperties>,
    private val settings: PayOrderAdmissionProperties,
) : PayOrderRequestQueue, PaymentCallAdmission, AutoCloseable {
    private val accounts = paymentAccounts.toList().also { selected ->
        require(selected.isNotEmpty()) { "At least one payment account must be available" }
        require(selected.map { it.accountName }.distinct().size == selected.size) { "Payment accounts must be unique" }
        require(selected.all { it.parallelRequests > 0 && it.rateLimitPerSec > 0 && it.averageProcessingTime.toMillis() > 0 }) {
            "Payment account limits and processing times must be positive"
        }
    }
    private val accountsByName = accounts.associateBy { it.accountName }
    private val processingBudgetMs = accounts.sumOf { it.averageProcessingTime.toMillis() } +
        if (accounts.size > 1) accounts.size * 1_000L else 0L
    private val processingTimeReserveMs = ceil(processingBudgetMs * settings.processingTimeReserveFactor).toLong()
    private val maxConcurrent = minOf(OrderPayer.PAYMENT_WORKER_COUNT, accounts.minOf { it.parallelRequests })
    private val sustainableRatePerSecond = accounts.minOf { account ->
        minOf(account.rateLimitPerSec.toDouble(), account.parallelRequests * 1_000.0 / processingBudgetMs)
    }.coerceAtMost(maxConcurrent * 1_000.0 / processingBudgetMs)
    private val rateWindows = accounts.associate { it.accountName to ArrayDeque<Long>() }
    private val actualCallWindows = accounts.associate { it.accountName to ArrayDeque<Long>() }
    private val retrySlotIntervalNanos = ceil(TimeUnit.SECONDS.toNanos(1) / sustainableRatePerSecond).toLong().coerceAtLeast(1)
    private val lock = ReentrantLock()
    private val changed = lock.newCondition()
    private val requests = ArrayDeque<QueuedRequest>()
    private var nextRetrySlotNanos = 0L
    private val dispatcher = Executors.newSingleThreadExecutor(NamedThreadFactory("pay-order-admission"))
    private val handoff = Executors.newFixedThreadPool(maxConcurrent, NamedThreadFactory("pay-order-handoff"))
    private val inFlight = AtomicInteger(0)
    private val closed = AtomicBoolean(false)
    private val rejectedByCapacity = Metrics.counter("payment_admission_rejected", "reason", "capacity")
    private val rejectedByDeadline = Metrics.counter("payment_admission_rejected", "reason", "deadline")
    private val retryAfter = Metrics.summary("payment_admission_retry_after_seconds")
    private val queueWait = Metrics.timer("payment_admission_wait")

    init {
        require(settings.maxQueueSize > 0) { "Pay order queue capacity must be positive" }
        require(settings.safetyMarginFactor.isFinite() && settings.safetyMarginFactor >= 0.0 && settings.safetyMarginFactor < 1.0) {
            "Pay order safety margin factor must be finite and between 0 (inclusive) and 1 (exclusive)"
        }
        require(settings.processingTimeReserveFactor.isFinite() && settings.processingTimeReserveFactor >= 1.0) {
            "Pay order processing time reserve factor must be finite and at least 1"
        }
        require(settings.retryAfter.seconds > 0 && settings.retryAfter.nano == 0) {
            "Retry-After must be a positive whole number of seconds"
        }

        Metrics.gauge("payment_admission_queued", this) { it.queueSize().toDouble() }
        Metrics.gauge("payment_admission_inflight", inFlight) { it.get().toDouble() }
        dispatcher.execute(::dispatchRequests)
    }

    override fun <T : Any> submit(deadline: Long, action: (onFinished: () -> Unit) -> T): DeferredResult<T> {
        val nowMs = System.currentTimeMillis()
        val safetyMarginMs = ceil((deadline - nowMs).coerceAtLeast(0).toDouble() * settings.safetyMarginFactor).toLong()
        val result = DeferredResult<T>((deadline - nowMs).coerceAtLeast(1))
        val request = QueuedRequest(deadline, safetyMarginMs, {
            val released = AtomicBoolean(false)
            val onFinished = {
                if (released.compareAndSet(false, true)) releaseSlot()
            }
            try {
                result.setResult(action(onFinished))
            } catch (exception: Throwable) {
                onFinished()
                result.setErrorResult(exception)
            }
        }, { retryAfterSeconds ->
            result.setErrorResult(rateLimitResponse(retryAfterSeconds))
        })

        result.onTimeout { cancelRequest(request, true) }
        result.onCompletion { cancelRequest(request, false) }

        val admissionNowMs = System.currentTimeMillis()
        val rejectionReason = lock.withLock {
            when {
                closed.get() -> "capacity"
                !hasProcessingTime(deadline, admissionNowMs, safetyMarginMs) -> "deadline"
                requests.size >= settings.maxQueueSize -> "capacity"
                !canAdmit(deadline, admissionNowMs, safetyMarginMs, System.nanoTime()) -> "deadline"
                else -> {
                    requests.addLast(request)
                    changed.signal()
                    null
                }
            }
        }
        if (rejectionReason != null) reject(request, rejectionReason)
        return result
    }

    override fun awaitCall(accountName: String, deadline: Long): Boolean {
        val account = accountsByName.getValue(accountName)
        val timestamps = actualCallWindows.getValue(accountName)
        try {
            lock.withLock {
                while (!closed.get()) {
                    val remainingMs = deadline - System.currentTimeMillis() - account.averageProcessingTime.toMillis()
                    if (remainingMs <= 0) return false

                    val nowNanos = System.nanoTime()
                    while (timestamps.isNotEmpty() && nowNanos - timestamps.first >= TimeUnit.SECONDS.toNanos(1)) {
                        timestamps.removeFirst()
                    }
                    if (timestamps.size < account.rateLimitPerSec) {
                        timestamps.addLast(nowNanos)
                        return true
                    }

                    val nextPermitNanos = timestamps.first + TimeUnit.SECONDS.toNanos(1) - nowNanos
                    changed.awaitNanos(minOf(nextPermitNanos, TimeUnit.MILLISECONDS.toNanos(remainingMs)))
                }
            }
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        }
        return false
    }

    private fun canAdmit(deadline: Long, nowMs: Long, safetyMarginMs: Long, nowNanos: Long): Boolean {
        expireRateWindows(nowNanos)
        val ahead = requests.size
        val freeSlots = maxConcurrent - inFlight.get()
        val concurrencyWaves = if (ahead < freeSlots) 0 else
            (ahead + 1 - freeSlots + maxConcurrent - 1) / maxConcurrent
        val concurrencyWaitMs = concurrencyWaves.toLong() * processingBudgetMs
        val rateWaitMs = accounts.maxOf { account ->
            val timestamps = rateWindows.getValue(account.accountName)
            val rate = account.rateLimitPerSec
            val permit = ahead + 1 - (rate - timestamps.size)
            if (permit <= 0) 0L else {
                val index = (permit - 1) % rate
                val cycles = (permit - 1) / rate + 1
                val stampNanos = if (index < timestamps.size) timestamps.elementAt(index) else nowNanos
                TimeUnit.NANOSECONDS.toMillis((stampNanos + cycles * TimeUnit.SECONDS.toNanos(1) - nowNanos).coerceAtLeast(0))
            }
        }
        val predictedWaitMs = maxOf(concurrencyWaitMs, rateWaitMs)
        return predictedWaitMs + processingTimeReserveMs + safetyMarginMs <= deadline - nowMs
    }

    private fun dispatchRequests() {
        try {
            while (!closed.get()) {
                val expired = mutableListOf<QueuedRequest>()
                var selected: QueuedRequest? = null
                lock.withLock {
                    while (!closed.get() && selected == null && expired.isEmpty()) {
                        val nowMs = System.currentTimeMillis()
                        val iterator = requests.iterator()
                        while (iterator.hasNext()) {
                            val request = iterator.next()
                            if (!request.isPending()) {
                                iterator.remove()
                            } else if (!hasProcessingTime(request.deadline, nowMs, request.safetyMarginMs)) {
                                iterator.remove()
                                expired.add(request)
                            }
                        }

                        val nowNanos = System.nanoTime()
                        if (expired.isEmpty() && requests.isNotEmpty() && canStart(nowNanos)) {
                            val request = requests.removeFirst()
                            if (request.markDispatched()) {
                                reserveSlot(nowNanos)
                                queueWait.record(nowNanos - request.enqueuedAtNanos, TimeUnit.NANOSECONDS)
                                selected = request
                            }
                        }
                        if (selected == null && expired.isEmpty() && !closed.get()) {
                            changed.awaitNanos(nextDispatchCheckNanos(System.nanoTime()))
                        }
                    }
                }
                expired.forEach { reject(it, "deadline") }
                selected?.let { request ->
                    try {
                        handoff.execute(request::execute)
                    } catch (_: RejectedExecutionException) {
                        reject(request, "capacity")
                    }
                }
            }
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }

    private fun hasProcessingTime(deadline: Long, nowMs: Long, safetyMarginMs: Long): Boolean =
        deadline - nowMs > processingTimeReserveMs + safetyMarginMs

    private fun canStart(nowNanos: Long): Boolean {
        if (inFlight.get() >= maxConcurrent) return false
        expireRateWindows(nowNanos)
        return accounts.all { rateWindows.getValue(it.accountName).size < it.rateLimitPerSec }
    }

    private fun nextDispatchCheckNanos(nowNanos: Long): Long {
        val maxWaitNanos = TimeUnit.MILLISECONDS.toNanos(10)
        if (requests.isEmpty() || inFlight.get() >= maxConcurrent) return maxWaitNanos
        val permitNanos = accounts.maxOf { account ->
            val timestamps = rateWindows.getValue(account.accountName)
            if (timestamps.size < account.rateLimitPerSec) 0L
            else timestamps.first + TimeUnit.SECONDS.toNanos(1) - nowNanos
        }
        return permitNanos.coerceIn(1, maxWaitNanos)
    }

    private fun expireRateWindows(nowNanos: Long) {
        accounts.forEach { account ->
            val timestamps = rateWindows.getValue(account.accountName)
            while (timestamps.isNotEmpty() && nowNanos - timestamps.first >= TimeUnit.SECONDS.toNanos(1)) {
                timestamps.removeFirst()
            }
        }
    }

    private fun reserveSlot(nowNanos: Long) {
        accounts.forEach { rateWindows.getValue(it.accountName).addLast(nowNanos) }
        inFlight.incrementAndGet()
    }

    private fun releaseSlot() {
        lock.withLock {
            inFlight.decrementAndGet()
            changed.signal()
        }
    }

    private fun cancelRequest(request: QueuedRequest, timedOut: Boolean) {
        lock.withLock {
            requests.remove(request)
            changed.signal()
        }
        if (timedOut) {
            reject(request, "deadline")
        } else if (request.stopBeforeExecution() == 1) {
            releaseSlot()
        }
    }

    private fun reject(request: QueuedRequest, reason: String) {
        val previousState = request.stopBeforeExecution()
        if (previousState >= 0) {
            if (previousState == 1) releaseSlot()
            val retryAfterSeconds = retryAfterSeconds()
            retryAfter.record(retryAfterSeconds.toDouble())
            if (reason == "capacity") rejectedByCapacity.increment() else rejectedByDeadline.increment()
            request.reject(retryAfterSeconds)
        }
    }

    private fun retryAfterSeconds(): Long = lock.withLock {
        val nowNanos = System.nanoTime()
        expireRateWindows(nowNanos)
        val rateWaitNanos = accounts.maxOf { account ->
            val timestamps = rateWindows.getValue(account.accountName)
            if (timestamps.size < account.rateLimitPerSec) 0L else
                (timestamps.first + TimeUnit.SECONDS.toNanos(1) - nowNanos).coerceAtLeast(0)
        }
        val slotWaitNanos = if (inFlight.get() >= maxConcurrent) TimeUnit.MILLISECONDS.toNanos(processingBudgetMs) else 0L
        val queueWaitNanos = ceil(requests.size * TimeUnit.SECONDS.toNanos(1) / sustainableRatePerSecond).toLong()
        val earliestRetryNanos = nowNanos + maxOf(settings.retryAfter.toNanos(), rateWaitNanos, slotWaitNanos, queueWaitNanos)
        val scheduledRetryNanos = maxOf(earliestRetryNanos, nextRetrySlotNanos)
        nextRetrySlotNanos = scheduledRetryNanos + retrySlotIntervalNanos
        ceil((scheduledRetryNanos - nowNanos).toDouble() / TimeUnit.SECONDS.toNanos(1)).toLong().coerceAtLeast(1)
    }

    private fun rateLimitResponse(retryAfterSeconds: Long): ResponseEntity<Void> = ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
        .header(HttpHeaders.RETRY_AFTER, retryAfterSeconds.toString())
        .build()

    private fun queueSize(): Int = lock.withLock { requests.size }

    @PreDestroy
    override fun close() {
        val pending = lock.withLock {
            if (!closed.compareAndSet(false, true)) return
            val remaining = requests.toList()
            requests.clear()
            changed.signalAll()
            remaining
        }
        dispatcher.shutdownNow()
        handoff.shutdown()
        pending.forEach { reject(it, "capacity") }
    }

    private class QueuedRequest(
        val deadline: Long,
        val safetyMarginMs: Long,
        private val executeAction: () -> Unit,
        private val rejectAction: (Long) -> Unit,
    ) {
        val enqueuedAtNanos = System.nanoTime()
        private val state = AtomicInteger(0)

        fun isPending(): Boolean = state.get() == 0
        fun markDispatched(): Boolean = state.compareAndSet(0, 1)
        fun execute() {
            if (state.compareAndSet(1, 3)) executeAction()
        }
        fun reject(retryAfterSeconds: Long) = rejectAction(retryAfterSeconds)
        fun stopBeforeExecution(): Int {
            while (true) {
                val current = state.get()
                if (current != 0 && current != 1) return -1
                if (state.compareAndSet(current, 2)) return current
            }
        }
    }
}
