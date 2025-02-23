package ru.quipy.payments.scheduled

import kotlinx.coroutines.runBlocking
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import ru.quipy.payments.logic.PaymentExternalSystemAdapter
import kotlin.concurrent.thread

@Service
class ScheduledExecutor(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) {
    private val isRunningState = HashMap<String, Boolean>()

    init {
        for (account in paymentAccounts) {
            isRunningState.put(account.name(), false)
        }
    }

    @Scheduled(fixedRate = 1000)
    fun acceptPayments() {
        for (account in paymentAccounts) {
            if (!isRunningState[account.name()]!!) {
                isRunningState[account.name()] = true

                thread {
                    runBlocking {
                        account.startSubmitPayments()
                    }
                }
            }

            account.submitPayments()
        }
    }
}