package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

@Component
class MetricsReporter {

    private val logger = LoggerFactory.getLogger(MetricsReporter::class.java)

    private val incomingRequests = AtomicLong(0)
    private val outgoingRequests = AtomicLong(0)
    private val completedRequests = AtomicLong(0)
    private val failedRequests = AtomicLong(0)

    @Autowired
    private lateinit var meterRegistry: MeterRegistry

    private lateinit var incomingCounter: Counter
    private lateinit var outgoingCounter: Counter
    private lateinit var completedCounter: Counter
    private lateinit var failedCounter: Counter

    @Autowired
    fun initMetrics() {
        incomingCounter = Counter.builder("payment.requests.incoming")
            .description("Total incoming payment requests")
            .register(meterRegistry)

        outgoingCounter = Counter.builder("payment.requests.outgoing")
            .description("Total outgoing payment requests to external systems")
            .register(meterRegistry)

        completedCounter = Counter.builder("payment.requests.completed")
            .description("Total completed payment requests")
            .register(meterRegistry)

        failedCounter = Counter.builder("payment.requests.failed")
            .description("Total failed payment requests")
            .register(meterRegistry)
    }

    fun incrementIncoming() {
        incomingRequests.incrementAndGet()
        incomingCounter.increment()
    }

    fun incrementOutgoing() {
        outgoingRequests.incrementAndGet()
        outgoingCounter.increment()
    }

    fun incrementCompleted() {
        completedRequests.incrementAndGet()
        completedCounter.increment()
    }

    fun incrementFailed() {
        failedRequests.incrementAndGet()
        failedCounter.increment()
    }

    @Scheduled(fixedRate = 10, timeUnit = TimeUnit.SECONDS)
    fun reportMetrics() {
        val incoming = incomingRequests.get()
        val outgoing = outgoingRequests.get()
        val completed = completedRequests.get()
        val failed = failedRequests.get()

        logger.info(
            "METRICS | Incoming: $incoming | Outgoing: $outgoing | Completed: $completed | Failed: $failed"
        )
    }
}