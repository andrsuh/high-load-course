package ru.quipy.common.utils

import okhttp3.Interceptor
import okhttp3.Response
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.SocketTimeoutException
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
            while (!rateLimiter.tick()) {
                if (timeoutAt - System.currentTimeMillis() <= 0)
                    throw SocketTimeoutException()
            }
            try {

                if (retryCount > 0) {
                    logger.info("Retry attempt $retryCount for request to ${request.url}")
                }

                while (true) {
                    val windowResponse = ongoingWindow.putIntoWindow()
                    if (windowResponse.isSuccess()) {
                        break
                    }
                    if (timeoutAt - System.currentTimeMillis() <= 0) {
                        throw SocketTimeoutException()
                    }
                }

                response = chain.proceed(request)

                if (response.isSuccessful || !isRetryable(response.code)) {

                    return response
                } else {
                    response.close()

                    if (retryCount == maxRetries) {
                        break
                    }

                    val delayTime = calculateDelayTime(retryCount, response.code)
                    logger.info("Waiting for ${delayTime}ms before next retry for code ${response.code}")

                    retryCount++
                    Thread.sleep(delayTime)
                }
            } catch (e: IOException) {
                if (retryCount == maxRetries) {
                    throw e
                }

                val delayTime = calculateDelayTime(retryCount, response?.code)
                logger.info("I/O error, waiting for ${delayTime}ms before next retry: ${e.message}")
                Thread.sleep(delayTime)
                retryCount++
            }
            finally {
                ongoingWindow.releaseWindow()
            }
        }
        return response ?: throw IOException("Failed after $maxRetries retry attempts")
    }

    private fun isRetryable(statusCode: Int): Boolean {
        return statusCode in retryableClientErrorCodes || (statusCode in 500..599) // все ошибки сервера ретраим
    }

    private fun calculateDelayTime(retryCount: Int, responseCode: Int?): Long {
        val delay = (initialDelayMillis * delayMultiplier.pow(retryCount.toDouble())).toLong()
        when (responseCode) {
            400, 401, 403, 404, 405 -> {
                throw RuntimeException("Client error code: ${responseCode}")
            }

            500 -> return 0
        }

        // return (exponentialDelay + jitter).coerceAtLeast(0)
        return delay
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RetryInterceptor::class.java)
    }
}
