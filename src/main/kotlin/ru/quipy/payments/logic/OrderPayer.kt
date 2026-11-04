package ru.quipy.payments.logic

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics
import jakarta.annotation.PostConstruct
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.server.ResponseStatusException
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val incomingRateLimiter = TokenBucketRateLimiter(
        rate = 11,
        bucketMaxCapacity = 220,
        window = 1,
        timeUnit = TimeUnit.SECONDS,
    )

    private val paymentExecutor = ThreadPoolExecutor(
        110,
        110,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(200),
        NamedThreadFactory("payment-submission-executor"),
        ThreadPoolExecutor.AbortPolicy()
    )

    @Autowired
    lateinit var meterRegistry: MeterRegistry

    @PostConstruct
    fun initMetrics() = ExecutorServiceMetrics.monitor(meterRegistry, paymentExecutor, "payment-submission-executor")

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        meterRegistry.counter("shop_payments_incoming_total").increment()
        val createdAt = System.currentTimeMillis()
        if (!incomingRateLimiter.tick()) {
            throw tooManyRequests("Payment rate limit exceeded")
        }
        try {
            paymentExecutor.submit {
                val createdEvent = paymentESService.create {
                    it.create(
                        paymentId,
                        orderId,
                        amount
                    )
                }
                logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
            }
        } catch(e: RejectedExecutionException) {
            throw tooManyRequests("Payment queue is full")
        }
        return createdAt
    }

    private fun tooManyRequests(reason: String) =
        object : ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS, reason) {
            override fun getHeaders() = HttpHeaders().apply {
                set(HttpHeaders.RETRY_AFTER, (System.currentTimeMillis() + 100).toString())
            }
        }
}
