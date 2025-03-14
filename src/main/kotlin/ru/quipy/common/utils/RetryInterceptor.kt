package ru.quipy.common.utils

import okhttp3.Interceptor
import java.io.IOException
import java.time.Duration

class RetryInterceptor(private val maxRetries: Int = 1, private val retryableHttpCodes: List<Int>) : Interceptor {
    private val initialDelay = Duration.ofSeconds(1)

    @Throws(IOException::class)
    override fun intercept(chain: Interceptor.Chain): okhttp3.Response {
        val request: okhttp3.Request = chain.request()
        var response: okhttp3.Response?
        var exception: IOException? = null
        var attempt = 0
        val delay = initialDelay

        while (attempt < maxRetries) {
            try {
                response = chain.proceed(request)

                if (response.isSuccessful || response.code !in retryableHttpCodes) {
                    return response
                }
            } catch (e: IOException) {
                exception = e
            } finally {
                attempt++
            }

            if (attempt < maxRetries) {
                try {
                    Thread.sleep(delay)
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                }
            }
            delay.multipliedBy(2)
        }

        if (exception != null) {
            throw exception
        }

        throw IOException("Maximum retry attempts ($maxRetries) exhausted")
    }
}