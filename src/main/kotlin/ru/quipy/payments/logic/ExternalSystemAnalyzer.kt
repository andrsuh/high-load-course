package ru.quipy.payments.logic

import ru.quipy.payments.dto.FinishedRequest
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.ceil

class ExternalSystemAnalyzer {
    private val maxAnalyzingRequests = 80
    private val finishedRequests = arrayOfNulls<FinishedRequest>(maxAnalyzingRequests)
    private var nextFinishedRequestPointer = AtomicInteger(0)

    fun addFinished(request: FinishedRequest) {
        finishedRequests[nextFinishedRequestPointer.getAndSet((nextFinishedRequestPointer.get() + 1) % maxAnalyzingRequests)] = request
    }

    fun getQuantile(quantile: Double): FinishedRequest? {
        if (quantile > 1) {
            throw IllegalArgumentException("Quantile must be lte 1")
        }

        if (finishedRequests.filterNotNull().size < 40) {
            // not enough data to calculate quantile
            return null
        }

        val sortedFinishedRequests = finishedRequests.sortedBy { it?.processingDuration }.filterNotNull()
        val q = ceil(quantile * sortedFinishedRequests.size)

        if (q >= sortedFinishedRequests.size) {
            return sortedFinishedRequests[sortedFinishedRequests.size - 1]
        }

        return sortedFinishedRequests[q.toInt()]
    }
}