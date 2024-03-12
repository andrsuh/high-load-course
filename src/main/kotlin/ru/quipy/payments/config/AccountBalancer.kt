package ru.quipy.payments.config

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.quipy.payments.logic.ExternalServiceProperties
import ru.quipy.payments.logic.PaymentExternalServiceImpl
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

@Service
class AccountBalancer {

    companion object {
        // Ниже приведены готовые конфигурации нескольких аккаунтов провайдера оплаты.
        // Заметьте, что каждый аккаунт обладает своими характеристиками и стоимостью вызова.

        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

        private val accountProps_1 = ExternalServiceProperties(
            // most expensive. Call costs 100
            "test",
            "default-1",
            parallelRequests = 10000,
            rateLimitPerSec = 100,
            request95thPercentileProcessingTime = Duration.ofMillis(1000),
        )

        private val accountProps_2 = ExternalServiceProperties(
            // Call costs 70
            "test",
            "default-2",
            parallelRequests = 100,
            rateLimitPerSec = 30,
            request95thPercentileProcessingTime = Duration.ofMillis(10_000),
        )

        private val accountProps_3 = ExternalServiceProperties(
            // Call costs 40
            "test",
            "default-3",
            parallelRequests = 30,
            rateLimitPerSec = 8,
            request95thPercentileProcessingTime = Duration.ofMillis(10_000),
        )

        private val accountProps_4 = ExternalServiceProperties(
            // Call costs 30
            "test",
            "default-4",
            parallelRequests = 8,
            rateLimitPerSec = 5,
            request95thPercentileProcessingTime = Duration.ofMillis(10_000),
        )

        private val accounts: Map<ExternalServiceProperties, AtomicLong> = mapOf(
            accountProps_1 to AtomicLong(0),
            accountProps_2 to AtomicLong(0),
            accountProps_3 to AtomicLong(0),
            accountProps_4 to AtomicLong(0)
        )
    }

    fun getAccount(): ExternalServiceProperties {

        while (true) {
            val accountProps4Count = accounts[accountProps_4]!!.get()
            val accountProps3Count = accounts[accountProps_3]!!.get()
            val accountProps2Count = accounts[accountProps_2]!!.get()
            val accountProps1Count = accounts[accountProps_1]!!.get()

            if (accountProps4Count <= 5) {
                if (!accounts[accountProps_4]!!.compareAndSet(accountProps4Count, accountProps4Count + 1))
                    continue

                logger.error("Account 4 acquired $accountProps4Count")
                return accountProps_4
            } else if (accountProps3Count <= 8) {
                if (!accounts[accountProps_3]!!.compareAndSet(accountProps3Count, accountProps3Count + 1))
                    continue

                logger.error("Account 3 acquired $accountProps3Count")
                return accountProps_3
            } else if (accountProps2Count <= 30) {
                if (!accounts[accountProps_2]!!.compareAndSet(accountProps2Count, accountProps2Count + 1))
                    continue

                logger.error("Account 2 acquired $accountProps2Count")
                return accountProps_2
            } else {
                if (!accounts[accountProps_1]!!.compareAndSet(accountProps1Count, accountProps1Count + 1))
                    continue

                logger.error("Account 1 acquired $accountProps1Count")
                return accountProps_1
            }
        }
    }

    fun resize(accountProps: ExternalServiceProperties) {
        accounts[accountProps]?.decrementAndGet()
    }
}