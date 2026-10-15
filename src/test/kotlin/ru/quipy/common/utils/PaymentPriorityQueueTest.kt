package ru.quipy.common.utils

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

class PaymentPriorityQueueTest {
    private fun task(amount: Int, latestStart: Long, seq: Long) =
        PaymentTask(amount, latestStart, seq) {}

    @Test
    fun `price wins among eligible tasks and newest breaks ties`() {
        val q = PaymentPriorityQueue(4, clock = { 5_000 })
        val cheap = task(100, 60_000, 3)
        val expensive = task(500, 6_000, 1)
        val newerExpensive = task(500, 7_000, 2)
        q.offer(cheap); q.put(expensive); q.offer(newerExpensive, 1, TimeUnit.SECONDS)
        assertSame(newerExpensive, q.take())
        assertSame(expensive, q.poll())
        assertSame(cheap, q.poll(1, TimeUnit.SECONDS))
    }

    @Test
    fun `eligibility expires exactly at latest start and is reevaluated while queued`() {
        var now = 9_000L
        val q = PaymentPriorityQueue(4, clock = { now })
        val old = task(1000, 10_000, 1)
        val fresh = task(100, 20_000, 2)
        q.offer(old); q.offer(fresh)
        assertSame(old, q.peek())
        now = 10_000
        assertSame(fresh, q.poll())
        val expired = task(9999, 10_000, 3)
        val valid = task(1, 20_000, 4)
        q.offer(expired); q.offer(valid)
        assertSame(valid, q.take())
        assertSame(expired, q.take())
        assertSame(old, q.take())
    }

    @Test
    fun `capacity timeout removal and drain preserve queued tasks`() {
        val q = PaymentPriorityQueue(1, clock = { 0 })
        val a = task(1, 0, 1)
        q.put(a)
        assertFalse(q.offer(task(2, 0, 2), 1, TimeUnit.MILLISECONDS))
        assertEquals(0, q.remainingCapacity())
        val iterator = q.iterator(); iterator.next(); iterator.remove()
        assertEquals(1, q.remainingCapacity())
        q.offer(a)
        val drained = mutableListOf<Runnable>()
        assertEquals(1, q.drainTo(drained))
        assertSame(a, drained.single())
        assertNull(q.poll(1, TimeUnit.MILLISECONDS))
    }
}
