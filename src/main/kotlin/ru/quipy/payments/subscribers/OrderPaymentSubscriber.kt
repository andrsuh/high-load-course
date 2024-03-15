package ru.quipy.payments.subscribers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.orders.api.OrderAggregate
import ru.quipy.orders.api.OrderPaymentStartedEvent
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.config.ExternalServicesConfig
import ru.quipy.payments.logic.*
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import javax.annotation.PostConstruct

@Service
class OrderPaymentSubscriber {

    val logger: Logger = LoggerFactory.getLogger(OrderPaymentSubscriber::class.java)

    @Autowired
    lateinit var subscriptionsManager: AggregateSubscriptionsManager

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>


    @Autowired
    @Qualifier(ExternalServicesConfig.FIRST_PAYMENT_BEAN)
    private lateinit var paymentService: PaymentService

    private fun getIndex(): Int {
        return Random().nextInt(4)
    }

    private fun isReset() : Boolean {
        val value = Random().nextDouble()
        return value <= 0.2
    }

    @Autowired
    @Qualifier(ExternalServicesConfig.SECOND_PAYMENT_BEAN)
    private lateinit var secondPaymentService: PaymentService

    @Autowired
    @Qualifier(ExternalServicesConfig.THIRD_PAYMENT_BEAN)
    private lateinit var thirdPaymentService: PaymentService

    @Autowired
    @Qualifier(ExternalServicesConfig.FOURTH_PAYMENT_BEAN)
    private lateinit var fourthPaymentService: PaymentService

    @Autowired
    private lateinit var paymentServices: List<PaymentService>

    private val paymentExecutor = Executors.newFixedThreadPool(16, NamedThreadFactory("payment-executor"))

    private var nearestTimes = longArrayOf(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE)

    private fun getNearest() : Int {
        val minTime = nearestTimes.min()
        return nearestTimes.indexOf(minTime)
    }

    @PostConstruct
    fun init() {
        paymentServices = paymentServices.sortedBy { it.getCost }

        subscriptionsManager.createSubscriber(OrderAggregate::class, "payments:order-subscriber", retryConf = RetryConf(1, RetryFailedStrategy.SKIP_EVENT)) {
            `when`(OrderPaymentStartedEvent::class) { event ->
                paymentExecutor.submit {
                    val createdEvent = paymentESService.create {
                        it.create(
                            event.paymentId,
                            event.orderId,
                            event.amount
                        )
                    }
                    logger.info("Payment ${createdEvent.paymentId} for order ${event.orderId} created.")

                    outerCycle@ while(true) {
                        if (!isReset()) {
                            val index = getNearest()
                            if (paymentServices[index].window.putIntoWindow()::class == NonBlockingOngoingWindow.WindowResponse.Success::class) {
                                if (paymentServices[index].rateLimiter.tick()) {
                                    nearestTimes[index] =
                                        (System.currentTimeMillis() + (1.0 / paymentServices[index].getSpeed())).toLong()
                                    paymentServices[index].submitPaymentRequest(
                                        createdEvent.paymentId,
                                        event.amount,
                                        event.createdAt
                                    )
                                    break
                                } else {
                                    paymentServices[index].window.releaseWindow()
                                }
                            }
                        } else
                            for (index in paymentServices.indices) {
                                if (paymentServices[index].window.putIntoWindow()::class == NonBlockingOngoingWindow.WindowResponse.Success::class) {
                                    if (paymentServices[index].rateLimiter.tick()) {
                                        nearestTimes[index] =
                                            (System.currentTimeMillis() + (1.0 / paymentServices[index].getSpeed())).toLong()
                                        paymentServices[index].submitPaymentRequest(
                                            createdEvent.paymentId,
                                            event.amount,
                                            event.createdAt
                                        )
                                        break@outerCycle
                                    } else {
                                        paymentServices[index].window.releaseWindow()
                                    }
                                }
                            }
                    }
                }
            }
        }
    }
}