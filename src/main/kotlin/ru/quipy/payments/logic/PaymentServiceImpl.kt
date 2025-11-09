package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.quipy.common.utils.LeakingBucketRateLimiter
import java.time.Duration
import java.util.*

@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        for (account in paymentAccounts) {
            account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
        }
    }

    override fun getLeakingBucket(waitingTime: Duration): LeakingBucketRateLimiter {
        val bucketSize = paymentAccounts.sumOf {
            it.getRateLimit() * (waitingTime.toMillis() - (it.getProcessingTime().toMillis() * 2.5)) / 1000
        }
        return LeakingBucketRateLimiter(
            rate = paymentAccounts.sumOf { it.getRateLimit() },
            window = Duration.ofSeconds(1),
            bucketSize = bucketSize.toInt()
        )
    }
}