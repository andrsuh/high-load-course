package ru.quipy.payments.logic

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.payments.config.RateLimiterProperties
import ru.quipy.payments.config.ResilienceProperties
import ru.quipy.payments.config.ThreadPoolProperties
import java.time.Duration

@Configuration
class ResilienceBeans(
    private val resilienceProperties: ResilienceProperties,
    private val rateLimiterProperties: RateLimiterProperties,
    private val threadPoolProperties: ThreadPoolProperties
) {

    @Bean
    fun resiliencePolicy(): ResiliencePolicy {
        return ResiliencePolicy(
            timeout = timeoutPolicy(),
            retry = retryPolicy(),
            rateLimiter = rateLimiterPolicy(),
            threadPool = threadPoolPolicy()
        )
    }

    private fun retryPolicy(): RetryPolicy {
        return RetryPolicy(
            maxAttempts = resilienceProperties.maxAttempts,
            initialDelay = resilienceProperties.getInitialDelayDuration(),
            maxDelay = resilienceProperties.getMaxDelayDuration(),
            factor = resilienceProperties.delayFactor
        )
    }

    private fun timeoutPolicy(): TimeoutPolicy {
        return TimeoutPolicy(
            requestTimeout = resilienceProperties.getRequestTimeoutDuration()
        )
    }

    private fun rateLimiterPolicy(): RateLimiterPolicy {
        return RateLimiterPolicy(
            window = rateLimiterProperties.getWindowDuration()
        )
    }

    private fun threadPoolPolicy(): ThreadPoolPolicy {
        return ThreadPoolPolicy(
            keepAliveTime = threadPoolProperties.getKeepAliveTimeDuration()
        )
    }
}

data class ResiliencePolicy(
    val timeout: TimeoutPolicy,
    val retry: RetryPolicy,
    val rateLimiter: RateLimiterPolicy,
    val threadPool: ThreadPoolPolicy
)

data class TimeoutPolicy(
    val requestTimeout: Duration?
)

data class RetryPolicy(
    val maxAttempts: Int,
    val initialDelay: Duration,
    val maxDelay: Duration,
    val factor: Double
)

data class RateLimiterPolicy(
    val window: Duration
)

data class ThreadPoolPolicy(
    val keepAliveTime: Duration
)