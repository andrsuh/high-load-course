package ru.quipy.payments.logic

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*
import java.util.concurrent.Executors



@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>,
    private val retryQueue: RetryQueue
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }


    private val retryQueueWorker = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher()).launch {
        while (true) {
            try {
                val next = retryQueue.getNext()
                if (next.deadline < System.currentTimeMillis()) {
                    logger.warn("Payment request deadline expired: {}", next)
                    continue
                }
                submitPaymentRequest(next.paymentId, next.amount, next.paymentStartedAt, next.deadline)
                logger.info("Retrying: {}", next)
            } catch (e: Exception) {
                logger.error("Error while getting next retry payment request", e)
                delay(1000)
            }
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }


    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        if (deadline < System.currentTimeMillis()) {
            logger.warn("Payment request deadline expired: {}", paymentId)
            return
        }

        for (account in paymentAccounts) {
            account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
        }
    }
}