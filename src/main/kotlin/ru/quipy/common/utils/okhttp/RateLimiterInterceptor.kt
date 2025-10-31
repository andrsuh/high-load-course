package ru.quipy.common.utils.okhttp

import okhttp3.Interceptor
import okhttp3.Response
import ru.quipy.common.utils.RateLimiter

class RateLimiterInterceptor(
    private val limiter: RateLimiter,
) : Interceptor {
    override fun intercept(chain: Interceptor.Chain): Response {
        while (!limiter.tick()) {
            Thread.sleep(10)
        }

        return chain.proceed(chain.request())
    }
}
