package ru.quipy.common.utils.parallel

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withTimeout

class SemaphoreParallelLimiter(
    permits: Int
) : ParallelLimiter {

    private val semaphore = Semaphore(permits)

    override fun <T> queueCall(call: () -> T): T = runBlocking {
        semaphore.withPermit { call() }
    }

    override fun <T> queueCallWithTimeout(deadline: Long, call: () -> T): T = runBlocking {
        withTimeout(deadline - System.currentTimeMillis()) {
            semaphore.withPermit { call() }
        }
    }
}
