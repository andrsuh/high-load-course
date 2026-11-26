package ru.quipy.common.utils

import java.util.AbstractQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class PaymentTask(val amount: Int, val latestStart: Long, val sequence: Long,
                  private val action: () -> Unit) : Runnable {
    override fun run() = action()
}

class PaymentPriorityQueue(
    private val capacity: Int,
    private val clock: () -> Long = System::currentTimeMillis,
) : AbstractQueue<Runnable>(), BlockingQueue<Runnable> {
    private val lock = ReentrantLock()
    private val available = lock.newCondition()
    private val space = lock.newCondition()
    private val tasks = ArrayList<PaymentTask>()
    init { require(capacity > 0) }

    private fun best(): PaymentTask? {
        val now = clock()
        val eligible = tasks.filter { now < it.latestStart }
        return eligible.maxWithOrNull(compareBy<PaymentTask> { it.amount }.thenBy { it.sequence })
            ?: tasks.maxByOrNull { it.sequence }
    }

    private fun insert(task: Runnable) {
        tasks.add(task as PaymentTask)
        available.signal()
    }

    private fun extract(): Runnable? = best()?.also { tasks.remove(it); space.signal() }

    override val size: Int get() = lock.withLock { tasks.size }

    override fun remainingCapacity(): Int = lock.withLock { capacity - tasks.size }

    override fun offer(element: Runnable): Boolean = lock.withLock {
        if (tasks.size == capacity) false else { insert(element); true }
    }

    override fun put(element: Runnable) {
        lock.lockInterruptibly()
        try { while (tasks.size == capacity) space.await(); insert(element) }
        finally { lock.unlock() }
    }

    override fun offer(element: Runnable, timeout: Long, unit: TimeUnit): Boolean {
        var remaining = unit.toNanos(timeout)
        lock.lockInterruptibly()
        try {
            while (tasks.size == capacity) {
                if (remaining <= 0) return false
                remaining = space.awaitNanos(remaining)
            }
            insert(element); return true
        } finally { lock.unlock() }
    }

    override fun take(): Runnable {
        lock.lockInterruptibly()
        try { while (tasks.isEmpty()) available.await(); return extract()!! }
        finally { lock.unlock() }
    }

    override fun poll(timeout: Long, unit: TimeUnit): Runnable? {
        var remaining = unit.toNanos(timeout)
        lock.lockInterruptibly()
        try {
            while (tasks.isEmpty()) {
                if (remaining <= 0) return null
                remaining = available.awaitNanos(remaining)
            }
            return extract()
        } finally { lock.unlock() }
    }

    override fun poll(): Runnable? = lock.withLock { extract() }

    override fun peek(): Runnable? = lock.withLock { best() }
    
    override fun remove(element: Runnable): Boolean = lock.withLock {
        tasks.remove(element).also { if (it) space.signal() }
    }
    
    override fun iterator(): MutableIterator<Runnable> = lock.withLock {
        val snapshot = tasks.toList().iterator()
        object : MutableIterator<Runnable> {
            private var current: Runnable? = null
            override fun hasNext() = snapshot.hasNext()
            override fun next(): Runnable = snapshot.next().also { current = it }
            override fun remove() { remove(current ?: throw IllegalStateException()); current = null }
        }
    }
    
    override fun drainTo(target: MutableCollection<in Runnable>): Int = drainTo(target, Int.MAX_VALUE)
    
    override fun drainTo(target: MutableCollection<in Runnable>, maxElements: Int): Int = lock.withLock {
        require(target !== this)
        var count = 0
        while (count < maxElements && tasks.isNotEmpty()) {
            val task = best()!!
            target.add(task)
            tasks.remove(task)
            space.signal()
            count++
        }
        count
    }
}
