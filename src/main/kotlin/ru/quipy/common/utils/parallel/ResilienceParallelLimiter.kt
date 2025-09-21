package ru.quipy.common.utils.parallel

import io.github.resilience4j.bulkhead.BulkheadConfig
import io.github.resilience4j.bulkhead.BulkheadFullException
import io.github.resilience4j.bulkhead.BulkheadRegistry
import java.time.Duration

class ResilienceParallelLimiter(
    accountName: String,
    parallelCount: Int,
) : ParallelLimiter {

    private val bulkhead: io.github.resilience4j.bulkhead.Bulkhead

    init {
        val config: BulkheadConfig = BulkheadConfig.custom()
            .maxConcurrentCalls(parallelCount)
            .maxWaitDuration(Duration.ofMillis(5))
            .build()

        val registry = BulkheadRegistry.of(config)

        bulkhead = registry.bulkhead(accountName)
    }

    private fun tryAcquire(deadline: Long = 0): Boolean {
        var permission = bulkhead.tryAcquirePermission()
        while ((deadline == 0L || System.currentTimeMillis() < deadline) && !permission) {
            Thread.sleep(10)
            permission = bulkhead.tryAcquirePermission()
        }

        if (deadline == 0L || System.currentTimeMillis() < deadline) {
            return permission
        } else if (permission) {
            bulkhead.releasePermission()
        }
        return false
    }

    override fun <T> queueCall(call: () -> T): T {
        if (tryAcquire()) {
            runCatching {
                call()
            }.also {
                bulkhead.releasePermission()
            }
        }

        throw BulkheadFullException.createBulkheadFullException(bulkhead)
    }

    override fun <T> queueCallWithTimeout(deadline: Long, call: () -> T): T {
        if (tryAcquire(deadline)) {
            return runCatching {
                call()
            }.also {
                bulkhead.releasePermission()
            }.getOrThrow()
        }

        throw BulkheadFullException.createBulkheadFullException(bulkhead)
    }
}
