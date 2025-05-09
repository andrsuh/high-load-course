package ru.quipy.common.utils

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.coroutines.CoroutineContext
import kotlin.math.min

class Retryer(private val maxRetries: Int) {
    fun executeWithDeadlineAsync(coroutineCtx: CoroutineContext, deadline: Long, requestId: UUID, block: suspend () -> Unit) {
        CoroutineScope(coroutineCtx).launch {
            withTimeout(deadline - System.currentTimeMillis()) {
                for (i in 1..maxRetries) {
                    delay(1)
                    if (i > 1) {
                        println("$requestId: retrying ${i-1} time")
                    }
                    try {
                        block()
                    } catch (e: RetryableError) {
                        continue
                    }
                    if (i > 1) {
                        println("$requestId: finished retrying on ${i-1} time")
                    }
                    break
                }
            }
        }
    }
}

class RetryableError(message: String) : Exception(message)