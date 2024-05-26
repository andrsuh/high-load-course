package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors


@Service
class PaymentSystemImpl(
        private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
        val paymentOperationTimeout = Duration.ofSeconds(80)
    }

    private val paymentExecutor = Executors.newFixedThreadPool(2000, NamedThreadFactory("payment-submission-executor"))

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        val enabledAccounts = paymentAccounts.filter { it.isEnabled() }
        var index = 0;
        do {
            val account = enabledAccounts.get(index)
            if (account.isReady()) {
                paymentExecutor.submit {
                    account.performPaymentAsync(paymentId, amount, paymentStartedAt)
                }
                break
            }
            index = (index + 1) % enabledAccounts.size
        } while(true)
    }
}