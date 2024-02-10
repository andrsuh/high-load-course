package ru.quipy.payments.projections

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.index.Indexed
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Repository
import org.springframework.stereotype.Service
import ru.quipy.apigateway.APIController
import ru.quipy.streams.AggregateSubscriptionsManager
import java.util.*
import javax.annotation.PostConstruct

@Service
class PaymentLogProjection {

    val logger: Logger = LoggerFactory.getLogger(PaymentLogProjection::class.java)

    @Autowired
    private lateinit var financialLogRepository: FinancialLogRepository

    @Autowired
    lateinit var subscriptionsManager: AggregateSubscriptionsManager

    // it is not required for the fault-tolerance course
    @PostConstruct
    fun init() {
//        subscriptionsManager.createSubscriber(
//            PaymentAggregate::class,
//            "payment:payment-log-projection",
//            retryConf = RetryConf(1, RetryFailedStrategy.SKIP_EVENT)
//        ) {
//            `when`(PaymentProcessedEvent::class) { event ->
//                if (!event.success) return@`when`
//
//                scope.launch {
//                    financialLogRepository.save(
//                        FinancialLogRecord(
//                            event.paymentId,
//                            APIController.FinancialOperationType.WITHDRAW,
//                            event.amount,
//                            event.orderId.toString(),
//                            System.currentTimeMillis(),
//                            event.transactionId,
//                            event.reason
//                        )
//                    )
//                }
//            }
//        }
    }

    @Document
    data class FinancialLogRecord(
        @Id
        val paymentId: UUID,
        val type: APIController.FinancialOperationType,
        val amount: Int,
        @Indexed
        val orderId: String,
        val timestamp: Long,
        val txId: UUID?,
        val reason: String?
    )
}

@Repository
interface FinancialLogRepository : MongoRepository<PaymentLogProjection.FinancialLogRecord, UUID> {
    fun findAllByOrderId(orderId: String): List<PaymentLogProjection.FinancialLogRecord>
}
