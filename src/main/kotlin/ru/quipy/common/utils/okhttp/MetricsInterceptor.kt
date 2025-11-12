package ru.quipy.common.utils.okhttp

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import okhttp3.Interceptor
import okhttp3.Response
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class MetricsInterceptor(meterRegistry: MeterRegistry, account: String) : Interceptor {

    private val parallelRequests: AtomicInteger? = meterRegistry.gauge("parallel_requests", AtomicInteger(0))
    private val requestsProcessed = meterRegistry.counter("requests_processed")
    val timer: Timer = Timer.builder("account_payment_request_duration")
        .tag("account", account)
        .publishPercentileHistogram(true)
        .register(meterRegistry)

    override fun intercept(chain: Interceptor.Chain): Response {
        parallelRequests?.getAndIncrement()
        val start = System.nanoTime()
        val response = chain.proceed(chain.request())
        timer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS)
        parallelRequests?.getAndDecrement()
        requestsProcessed.increment()
        return response
    }
}