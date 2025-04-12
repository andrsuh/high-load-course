package ru.quipy.common.utils

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

class CountingErrorMeter(
    private val window: Int,
    private val minNumberInvocations: Long = 50
) {
    private val invocations: Array<Event?> = Array(window) { null }
    private val counter = AtomicInteger()

    private val total = AtomicLong(0)
    private val err = AtomicLong(0)

    fun onSuccess() {
        val index = counter.getAndIncrement()
        val current = invocations[index % window]
        when (current) {
            is Event.Failure -> {
                err.decrementAndGet()
                invocations[index % window] = Event.Success()
            }

            is Event.Success -> {
                // do nothing
            }

            null -> {
                total.incrementAndGet()
                invocations[index % window] = Event.Success()
            }
        }
    }


    fun onFailure() {
        val index = counter.getAndIncrement()
        val current = invocations[index % window]
        when (current) {
            is Event.Failure -> {
                // do nothing
            }

            is Event.Success -> {
                err.incrementAndGet()
                invocations[index % window] = Event.Failure()
            }

            null -> {
                total.incrementAndGet()
                err.incrementAndGet()
                invocations[index % window] = Event.Failure()
            }
        }
    }

    fun getAvgRatio(): Double {
        return if (total.get() < minNumberInvocations) 0.0 else err.get().toDouble() / total.get()
    }

    sealed class Event {
        class Failure : Event()
        class Success : Event()
    }
}