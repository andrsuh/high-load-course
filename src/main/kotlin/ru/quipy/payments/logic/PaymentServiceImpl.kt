package ru.quipy.payments.logic

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.common.utils.TooManyRequestsException
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.Semaphore
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import kotlin.collections.sumOf
import kotlin.concurrent.withLock
import kotlin.math.ceil
import kotlin.math.floor
import kotlin.math.max
import kotlin.math.min


@Service
class PaymentSystemImpl(
    private val meterRegistry: MeterRegistry,
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
        val tasksDefault = 1800.0

        val coffAdmit = 1.5
        val coffSend = 1.5
    }
    private val queueSize = AtomicInteger(0)
    private val accounts = paymentAccounts.map { Account(it) }
    private val threads = accounts.sumOf { it.threads }
    private val throughput = accounts.sumOf { it.throughput }
    private val maxProcessSec = accounts.maxOf { it.processSec }
    private val ordersInQueue = ceil(tasksDefault * throughput).toInt()

    private val executor = ThreadPoolExecutor(
        threads,
        threads,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(ordersInQueue),
        NamedThreadFactory("test"),
    )

    init {
        ExecutorServiceMetrics.monitor(
            meterRegistry,
            executor,
            "executor-payment-sys-main"
        )
    }

    override fun foo(deadline: Long) {
        val now = System.currentTimeMillis()
        val delta = deadline - now - maxProcessSec.toMillis() * coffAdmit
        val throughputMil = throughput / 1000.0 / coffAdmit
        val canProcessInQueue = ceil(throughputMil * delta)

        val temp = queueSize.decrementAndGet()
        val res =  temp - canProcessInQueue

        throw TooManyRequestsException(now + ((max(res, 1.0)) / throughputMil).toLong())
    }

    override fun checkDeadline(deadline: Long) {
        val now = System.currentTimeMillis()
        val delta = deadline - now - maxProcessSec.toMillis() * coffAdmit
        val throughputMil = throughput / 1000.0 / coffAdmit
        val canProcessInQueue = ceil(throughputMil * delta)

        val temp = queueSize.incrementAndGet()
        val res =  temp - canProcessInQueue

        val val1 = delta <= 0
        val val2 = res > 0
        val val3 = temp > ordersInQueue

        if (val1 || val2 || val3) {
            queueSize.decrementAndGet()
            throw TooManyRequestsException(now + ((max(res, 1.0)) / throughputMil).toLong())
        }
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        executor.submit {
            while (deadline - System.currentTimeMillis() - maxProcessSec.toMillis() * coffSend >= 0) {
                for (account in accounts) {
                    if (account.tryAcquire()) {
                        //TODO:
                        // местечко под обработку ошибки, пака было лень
                        queueSize.decrementAndGet()
                        try {
                            account.adapter.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
                        } finally {
                            account.release()
                        }
                        return@submit
                    }
                }
                Thread.sleep(10)
            }
            queueSize.decrementAndGet()
            logger.error("IMPOSSIBLE :)")
        }
    }

    private class Account(
        val adapter: PaymentExternalSystemAdapter
    ) {
        val processSec =  adapter.rapt()
        val threads = ceil(adapter.rpc() * (processSec.toMillis() / 1000.0))
            .toInt()
            .coerceIn(1, adapter.pr().toInt())
        val throughput = min(adapter.rpc(), (threads.toDouble() / (processSec.toMillis() / 1000.0)))
        private val sema = Semaphore(threads)
        private val limiter = SlidingWindowRateLimiter(adapter.rpc().toLong() , Duration.ofSeconds(1))

        fun tryAcquire(): Boolean {
            if (!sema.tryAcquire()) {
                return false
            }

            if (limiter.tick()) {
                return true
            }

            sema.release()
            return false
        }

        fun release() = sema.release()
    }
}