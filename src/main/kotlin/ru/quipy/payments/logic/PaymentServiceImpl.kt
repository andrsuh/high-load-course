package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import java.util.*
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.LockSupport


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)

        private const val MAX_ATTEMPTS = 3
        private const val IDLE_POLL_MILLIS = 2L
        private const val BUSY_PARK_NANOS = 1_000_000L
    }

    private val accountsByPrice = paymentAccounts.filter { it.isEnabled() }.sortedBy { it.price() }
    private val sequence = AtomicLong(0)
    private val pending = PriorityBlockingQueue<PaymentTask>(1024) { left, right ->
        val byDeadline = left.deadline.compareTo(right.deadline)
        if (byDeadline != 0) byDeadline else left.sequence.compareTo(right.sequence)
    }

    init {
        NamedThreadFactory("payment-dispatcher").newThread(::dispatchLoop).start()
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        pending.offer(PaymentTask(paymentId, amount, paymentStartedAt, deadline, sequence.incrementAndGet(), 1))
    }

    private fun dispatchLoop() {
        while (!Thread.currentThread().isInterrupted) {
            val task = awaitTask() ?: continue

            if (now() >= task.deadline) {
                logger.warn("Payment ${task.paymentId} dropped, deadline exceeded while waiting in the submission queue")
                continue
            }

            if (!tryDispatch(task)) {
                pending.offer(task)
                LockSupport.parkNanos(BUSY_PARK_NANOS)
            }
        }
    }

    private fun awaitTask(): PaymentTask? {
        return try {
            pending.poll(IDLE_POLL_MILLIS, TimeUnit.MILLISECONDS)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            null
        }
    }

    private fun tryDispatch(task: PaymentTask): Boolean {
        val cheapest = accountsByPrice.firstOrNull() ?: return false

        for (account in accountsByPrice) {
            if (account.tryReserve(task.deadline)) {
                send(account, task)
                return true
            }
            if (account === cheapest && !mustEscalate(task, cheapest)) {
                return false
            }
        }
        return false
    }

    private fun send(account: PaymentExternalSystemAdapter, task: PaymentTask) {
        try {
            account.submitPayment(task.paymentId, task.amount, task.paymentStartedAt, task.deadline) { succeeded ->
                onAttemptFinished(task, succeeded)
            }
        } catch (e: Exception) {
            logger.error("Failed to submit payment ${task.paymentId} through ${account.name()}", e)
            account.release()
            onAttemptFinished(task, false)
        }
    }

    private fun onAttemptFinished(task: PaymentTask, succeeded: Boolean) {
        if (succeeded || now() >= task.deadline || task.attempts >= MAX_ATTEMPTS) {
            return
        }
        pending.offer(task.copy(attempts = task.attempts + 1, sequence = sequence.incrementAndGet()))
    }

    private fun mustEscalate(task: PaymentTask, cheapest: PaymentExternalSystemAdapter): Boolean {
        val queueDelayMillis = pending.size.toLong() * 1000L / cheapest.ratePerSecond().coerceAtLeast(1)
        return now() + queueDelayMillis + cheapest.processingTime().toMillis() >= task.deadline
    }

    private data class PaymentTask(
        val paymentId: UUID,
        val amount: Int,
        val paymentStartedAt: Long,
        val deadline: Long,
        val sequence: Long,
        val attempts: Int,
    )
}
