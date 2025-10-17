package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import ru.quipy.apigateway.HttpMetrics
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Metrics

@Service
class OrderPayer{

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val linkedBlockingQueue = LinkedBlockingQueue<Runnable>(8000) 

    val threadQueueCounter: Gauge = Gauge.builder(
        "requests_in_thread_queue_total",
        java.util.function.Supplier { linkedBlockingQueue.size.toDouble() }
    )
        .description("Total number of payment requests in queue")
        .register(Metrics.globalRegistry)

    private val paymentExecutor = ThreadPoolExecutor(
        16,
        16,
        0L,
        TimeUnit.MILLISECONDS,
        linkedBlockingQueue,
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long,metrics: HttpMetrics): Triple<Long, Boolean, Long> {
        val createdAt = System.currentTimeMillis()
        val timeToProcessAllInQueue = (linkedBlockingQueue.size * 13) * 1000
        if ((createdAt + timeToProcessAllInQueue ) > deadline)
        {
            return Triple(createdAt,false,createdAt + timeToProcessAllInQueue)
        }
        paymentExecutor.submit {
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }
            logger.info("Payment ${createdEvent.paymentId} for order $orderId created.")

            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
            metrics.responceCounter.increment()
        }
        return Triple(createdAt,true,0)
    }
}