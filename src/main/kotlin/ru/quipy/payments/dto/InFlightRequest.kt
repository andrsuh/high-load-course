package ru.quipy.payments.dto

import kotlinx.coroutines.*
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.*
import java.lang.Runnable
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import kotlin.time.measureTimedValue

open class InFlightRequest(
    val previousStateRequest: PendingRequest,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val url: String,
    private val timeout: Long,
    private val exceptionHandler: (paymentRequest: PendingRequest, e: Throwable) -> Unit
) : Comparable<InFlightRequest>, Runnable {
    private val inFlightRequestsScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())

    override fun compareTo(other: InFlightRequest): Int {
        return previousStateRequest.compareTo(other.previousStateRequest)
    }

    var processingDuration = 0L

    @OptIn(ExperimentalCoroutinesApi::class)
    override fun run() {
        inFlightRequestsScope.launch {
            // 1. Параллельное выполнение логирования и запроса
            val logJob = launch {
                paymentESService.update(previousStateRequest.paymentId) {
                    val currentTime = now() // Вычисляем время один раз
                    it.logSubmission(
                        success = true,
                        previousStateRequest.transactionId,
                        currentTime,
                        Duration.ofMillis(currentTime - previousStateRequest.paymentStartedAt)
                    )
                }
            }

            // 2. Оптимизированный вызов с обработкой исключений
            try {
                withTimeout(timeout * 3) {
                    withContext(Dispatchers.IO.limitedParallelism(64)) { // Ограничиваем параллелизм
                        previousStateRequest.call(
                            url = url,
                            timeout = timeout,
                            responseHandler = ::responseHandler,
                            exceptionHandler = exceptionHandler
                        )
                    }
                }
            } catch (e: Exception) {
                exceptionHandler(previousStateRequest, e)
            } finally {
                logJob.join() // Гарантируем завершение логирования
            }
        }
    }

    private fun responseHandler(response: ExternalSysResponse) {
        paymentESService.update(previousStateRequest.paymentId) {
            it.logProcessing(response.result, now(), previousStateRequest.transactionId, reason = response.message)
        }
    }
}