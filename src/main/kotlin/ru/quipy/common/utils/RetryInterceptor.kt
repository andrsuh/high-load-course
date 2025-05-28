package ru.quipy.common.utils

import okhttp3.Interceptor
import okhttp3.Response
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.Random
import kotlin.math.pow

class RetryInterceptor(
    private val rateLimiter: RateLimiter,
    private val ongoingWindow: NonBlockingOngoingWindow,
    private val maxRetries: Int,
    private val initialDelayMillis: Long,
    private val timeoutInMillis: Long,
    private val delayMultiplier: Double,
    private val retryableClientErrorCodes: Set<Int>,
) : Interceptor {

    private val random = Random()

    override fun intercept(chain: Interceptor.Chain): Response {
        val timeoutAt = System.currentTimeMillis() + timeoutInMillis
        val request = chain.request()

        var retryCount = 0
        var response: Response? = null

        while (retryCount <= maxRetries && (timeoutAt - System.currentTimeMillis() > 0)) {
            try {

                if (retryCount > 0) {
                    logger.info("Retry attempt $retryCount for request to ${request.url}")
                }

                val windowResponse = ongoingWindow.putIntoWindow()
                if (windowResponse.isSuccess()) {
                    break
                }
                response = chain.proceed(request)

                if (response.isSuccessful || !isRetryable(response.code)) {
                    return response
                } else {
                    response.close()

                    if (retryCount == maxRetries) {
                        break
                    }

                    val delayTime = calculateDelayTime(retryCount)
                    logger.info("Waiting for ${delayTime}ms before next retry for code ${response.code}")

                    Thread.sleep(delayTime)
                    retryCount++
                }
            } catch (e: IOException) {
                if (retryCount == maxRetries) {
                    throw e
                }

                val delayTime = calculateDelayTime(retryCount)
                logger.info("I/O error, waiting for ${delayTime}ms before next retry: ${e.message}")
                Thread.sleep(delayTime)
                retryCount++
            }
        }
        return response ?: throw IOException("Failed after $maxRetries retry attempts")
    }

    private fun isRetryable(statusCode: Int): Boolean {
        return statusCode in retryableClientErrorCodes || (statusCode in 500..599) // все ошибки сервера ретраим
    }

    private fun calculateDelayTime(retryCount: Int): Long {
        val exponentialDelay = (initialDelayMillis * delayMultiplier.pow(retryCount.toDouble())).toLong()
        // // добавим рандома для некст кейса))
        // val jitterPercentage = 0.3
        // val jitter = (exponentialDelay * jitterPercentage * (random.nextDouble() * 2 - 1)).toLong()

        // return (exponentialDelay + jitter).coerceAtLeast(0)
        return exponentialDelay
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RetryInterceptor::class.java)
    }
}
