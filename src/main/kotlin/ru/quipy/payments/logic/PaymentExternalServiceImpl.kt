package ru.quipy.payments.logic

import ru.quipy.payments.logic.PaymentStages.Payment
import ru.quipy.payments.logic.PaymentStages.PaymentStage
import java.util.*


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val stageHead: PaymentStage<*, *>
) : PaymentExternalSystemAdapter {


    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val payment = Payment(paymentId, amount, paymentStartedAt, deadline)

        stageHead.process(payment)
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()