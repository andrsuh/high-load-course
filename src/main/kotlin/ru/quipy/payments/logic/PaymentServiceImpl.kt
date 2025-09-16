package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.FixedWindowRateLimiter
import ru.quipy.common.utils.NamedThreadFactory
import java.time.Duration
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    private val accountQueues = mutableMapOf<String, AccountQueueManager>()
    private val queueLock = ReentrantLock()

    init {
        paymentAccounts.forEach { account ->
            if (account.isEnabled()) {
                val properties = getAccountProperties(account)
                accountQueues[account.name()] = AccountQueueManager(account, properties)
                logger.info("Initialized queue for account: ${account.name()} with parallelRequests: ${properties.parallelRequests}, rateLimitPerSec: ${properties.rateLimitPerSec}")
            }
        }
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val selectedAccount = selectBestAccount(amount, deadline - paymentStartedAt)

        if (selectedAccount != null) {
            val queueManager = accountQueues[selectedAccount.name()]
            queueManager?.submitPayment(paymentId, amount, paymentStartedAt, deadline)
                ?: logger.error("No queue manager found for account: ${selectedAccount.name()}")
        } else {
            logger.warn("No suitable account found for payment: $paymentId, amount: $amount")
        }
    }

    private fun selectBestAccount(amount: Int, timeToDeadline: Long): PaymentExternalSystemAdapter? {
        return queueLock.withLock {
            accountQueues.values
                .filter { it.canAcceptPayment(timeToDeadline) }
                .minByOrNull { it.getEstimatedProcessingTime() + it.account.price() }
                ?.account
        }
    }

    private fun getAccountProperties(account: PaymentExternalSystemAdapter): PaymentAccountProperties {

        return PaymentAccountProperties(
            serviceName = "default",
            accountName = account.name(),
            parallelRequests = 20,
            rateLimitPerSec = 5,
            price = account.price(),
            averageProcessingTime = Duration.ofSeconds(11),
            enabled = account.isEnabled()
        )
    }

    private class AccountQueueManager(
        val account: PaymentExternalSystemAdapter,
        private val properties: PaymentAccountProperties
    ) {
        private val rateLimiter = FixedWindowRateLimiter(
            properties.rateLimitPerSec,
            1,
            TimeUnit.SECONDS
        )

        private val executor = ThreadPoolExecutor(
            properties.parallelRequests,
            properties.parallelRequests,
            0L,
            TimeUnit.MILLISECONDS,
            LinkedBlockingQueue(1000),
            NamedThreadFactory("payment-${properties.accountName}"),
            CallerBlockingRejectedExecutionHandler(Duration.ofMinutes(5))
        )

        private val semaphore = Semaphore(properties.parallelRequests)

        fun submitPayment(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
            executor.submit {
                try {
                    if (!rateLimiter.tick()) {
                        logger.warn("[${properties.accountName}] Rate limit exceeded for payment: $paymentId")
                        return@submit
                    }

                    semaphore.acquire()

                    try {
                        logger.info("[${properties.accountName}] Processing payment: $paymentId from queue")
                        account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
                    } finally {
                        semaphore.release()
                    }
                } catch (e: Exception) {
                    logger.error("[${properties.accountName}] Error processing payment: $paymentId", e)
                }
            }
        }

        fun canAcceptPayment(timeToDeadline: Long): Boolean {
            val queueSize = executor.queue.size
            val estimatedWaitTime = queueSize * properties.averageProcessingTime.toMillis() / properties.parallelRequests
            return estimatedWaitTime < timeToDeadline && rateLimiter.tick()
        }

        fun getEstimatedProcessingTime(): Long {
            val queueSize = executor.queue.size
            return queueSize * properties.averageProcessingTime.toMillis() / properties.parallelRequests
        }
    }
}
