package ru.quipy.payments.logic

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CustomSlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    val rateLimiterMap = paymentAccounts.associate {
        it.name() to CustomSlidingWindowRateLimiter(
            rate = 1,
            window = Duration.ofMillis(600)
        )
    }

    val semaphoresMap = paymentAccounts.associate {
        it.name() to Semaphore(it.parallelRequests())
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        for (account in paymentAccounts) {
            val rateLimiter = rateLimiterMap[account.name()]!!
            val semaphore = semaphoresMap[account.name()]!!

            runBlocking {
                semaphore.withPermit {
                    if (account.isDeadlineExceeded(deadline)) {
                        account.failPayment(paymentId)
                        throw IllegalStateException()
                    }
                    while (!rateLimiter.tick()) {
                        delay(10)
                    }
                    if (account.isDeadlineExceeded(deadline)) {
                        account.failPayment(paymentId)
                        throw IllegalStateException()
                    }
                    account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
                }
            }
        }
    }
}