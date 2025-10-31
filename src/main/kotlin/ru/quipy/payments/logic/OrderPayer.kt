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

    private fun processingSpeed(property : PaymentAccountProperties) : Double{
        return kotlin.math.min(property.rateLimitPerSec.toDouble(), property.parallelRequests.toDouble() / property.averageProcessingTime.toSeconds())
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long,metrics: HttpMetrics): Triple<Long, Boolean, Long> {
        val createdAt = System.currentTimeMillis()
        val canParallel = paymentService.getAccountsProperties().minOf { p -> processingSpeed(p)}
        val maxProcessingTime = paymentService.getAccountsProperties().minOf { p -> p.averageProcessingTime}

        val timeToProcessAllInQueue = ((linkedBlockingQueue.size.toDouble()) / canParallel) * (maxProcessingTime.toSeconds()) * 1000

        val canRestInQueue =  maxProcessingTime.toSeconds() /- 1.0
        val size = linkedBlockingQueue.size
        logger.info("Payment ${paymentId} for order $orderId created. timeToProcessAllInQueue $timeToProcessAllInQueue queueSize $size"  )
        if ((createdAt + timeToProcessAllInQueue ) > deadline)
        {
            logger.info("send too many requests becouse createdAt $createdAt + $timeToProcessAllInQueue > $deadline"  )
            metrics.toManyRequestsDelayTime2.record(timeToProcessAllInQueue.toLong(), TimeUnit.MILLISECONDS)
            return Triple(createdAt,false,createdAt + (timeToProcessAllInQueue - canRestInQueue*1000).toLong())
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


    fun getAccountsProperties() : List<PaymentAccountProperties> {
        return paymentService.getAccountsProperties()
    }

    fun getNumberOfRequests(): Long {
        return (linkedBlockingQueue.size + paymentService.getNumberOfRequests()).toLong()
    }
}