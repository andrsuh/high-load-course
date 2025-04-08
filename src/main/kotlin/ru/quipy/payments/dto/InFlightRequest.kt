package ru.quipy.payments.dto

import kotlinx.coroutines.*
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.*
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors

open class InFlightRequest(
    val previousStateRequest: PendingRequest,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val timeout: Long,
    private val exceptionHandler: (paymentRequest: PendingRequest, e: Throwable) -> Unit
) : Comparable<InFlightRequest> {
    override fun compareTo(other: InFlightRequest): Int {
        return previousStateRequest.compareTo(other.previousStateRequest)
    }

    fun run(): CompletableFuture<Unit> {
        paymentESService.update(previousStateRequest.paymentId) {
            val currentTime = now()
            it.logSubmission(
                success = true,
                previousStateRequest.transactionId,
                currentTime,
                Duration.ofMillis(currentTime - previousStateRequest.paymentStartedAt)
            )
        }

        // 2. Параллельно выполняем основной вызов
        val callFuture = previousStateRequest.call(
            timeout = timeout,
            responseHandler = ::responseHandler,
            exceptionHandler = exceptionHandler
        )

        // 3. Комбинируем результаты
        return callFuture
            .thenApply { Unit } // Явное преобразование Void в Unit
            .exceptionally { e ->
                exceptionHandler(previousStateRequest, e ?: RuntimeException("Unknown error"))
                Unit
            }
    }

    private fun responseHandler(response: ExternalSysResponse) {
        paymentESService.update(previousStateRequest.paymentId) {
            it.logProcessing(response.result, now(), previousStateRequest.transactionId, reason = response.message)
        }
    }
}