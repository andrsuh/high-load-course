package ru.quipy.payments.logic

import java.io.FileWriter
import java.io.IOException
import java.util.*

data class RequestData(val responseTime: Long, val httpCode: Int, val paymentId: UUID, val transactionId: UUID)

class RequestCSVService {
    private val requestDataList = mutableListOf<RequestData>() // Список с данными по запросам
    private val finalRequestNumber = 1000;

    @Synchronized
    fun addRequestData(responseTime: Long, httpCode: Int, paymentId: UUID, transactionId: UUID) {
        requestDataList.add(RequestData(responseTime, httpCode, paymentId, transactionId))
        if (requestDataList.size % 50 == 0 || requestDataList.size == 2) {
            saveToCsv("request_stats_" + requestDataList.size + ".csv")
        }
    }

    fun saveToCsv(fileName: String = "request_stats.csv") {
        try {
            FileWriter(fileName).use { writer ->
                writer.write("Response Time (ms), HTTP Code, PaymentID, TransactionID\n") // Заголовок
                for (data in requestDataList) {
                    writer.write("${data.responseTime}, ${data.httpCode}, ${data.paymentId}, ${data.transactionId}\n")
                }
            }
            println("Request statistics saved to $fileName")
        } catch (e: IOException) {
            println("Error saving request statistics: ${e.message}")
        }
    }
}