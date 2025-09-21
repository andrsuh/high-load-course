package ru.quipy.common.utils.parallel

interface ParallelLimiter {

    fun<T> queueCall(call: () -> T): T

    fun<T> queueCallWithTimeout(deadline: Long, call: () -> T): T
}
