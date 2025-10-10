package ru.quipy.common.utils.parallel

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.payments.logic.PaymentAccountProperties

@Configuration
class ParallelLimiterConfiguration {

    @Bean
    fun parallelLimiters(
        properties: List<PaymentAccountProperties>,
    ): Map<String, ParallelLimiter?> =
        properties
            .associateBy(PaymentAccountProperties::accountName)
            .mapValues {
                when (it.key) {
                    "acc-5" -> SemaphoreParallelLimiter(it.value.parallelRequests)
                    else -> null
                }
            }
}
