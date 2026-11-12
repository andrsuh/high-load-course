package ru.quipy.payments.logic

import io.micrometer.core.instrument.Metrics
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.ScheduledThreadPoolExecutor
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

    // Каждая принятая оплата ставится в очередь на свой забронированный момент отправки (см. processPayment).
    // Очередь планировщика упорядочена по этому моменту; её длину ограничивает бронирование по дедлайну.
    // Потоки заняты только самим вызовом провайдера, поэтому их число считаем по закону Литтла
    private val paymentExecutor by lazy {
        val threads = paymentService.parallelism()
        logger.info("Payment executor threads: $threads")
        ScheduledThreadPoolExecutor(threads, NamedThreadFactory("payment-submission-executor")).also { executor ->
            Metrics.gauge("shop_payment_queue_size", executor) { it.queue.size.toDouble() }
        }
    }

    private val acceptedPayments = Metrics.counter("shop_payment_requests", "result", "accepted")
    private val rejectedPayments = Metrics.counter("shop_payment_requests", "result", "rejected")

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        // Не берём оплату, которую не успеем провести до дедлайна: просим клиента прийти позже, вместо того чтобы платить провайдеру за опоздавший запрос
        val sendAt = paymentService.reserveSendSlot(deadline) ?: reject(deadline)

        val createdAt = System.currentTimeMillis()
        paymentExecutor.schedule({
            // Исключение внутри задачи планировщик молча сохранил бы в никем не читаемый Future, поэтому логируем сами
            try {
                val createdEvent = paymentESService.create {
                    it.create(
                        paymentId,
                        orderId,
                        amount
                    )
                }
                logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline, sendAt)
            } catch (e: Exception) {
                logger.error("Payment $paymentId for order $orderId failed", e)
            }
        }, sendAt - createdAt, TimeUnit.MILLISECONDS)
        acceptedPayments.increment()
        return createdAt
    }

    private fun reject(deadline: Long): Nothing {
        rejectedPayments.increment()
        throw PaymentRejectedException(paymentService.retryAfter(deadline))
    }
}

class PaymentRejectedException(val retryAfter: Long) : RuntimeException("Payment can't be processed before the deadline")