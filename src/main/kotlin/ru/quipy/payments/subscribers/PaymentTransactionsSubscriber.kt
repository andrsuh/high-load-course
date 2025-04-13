package ru.quipy.payments.subscribers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.api.PaymentProcessedEvent
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy

@Service
class PaymentTransactionsSubscriber {

    companion object {
        private const val MAX_LOG_ENTRIES = 100_000
        private const val CLEANUP_INTERVAL_MINUTES = 60L
    }

    val logger: Logger = LoggerFactory.getLogger(PaymentTransactionsSubscriber::class.java)

    // Оптимизированное хранилище
    private val paymentLog: ConcurrentHashMap<UUID, ConcurrentLinkedQueue<PaymentLogRecord>> = ConcurrentHashMap()
    private val batchQueue = LinkedBlockingQueue<PaymentLogRecord>()
    private var isRunning = true

    @Autowired
    lateinit var subscriptionsManager: AggregateSubscriptionsManager

    @PostConstruct
    fun init() {
        startBatchProcessor()
        startCleanupScheduler()

        subscriptionsManager.createSubscriber(
            PaymentAggregate::class,
            "payments:payment-processings-subscriber",
            retryConf = RetryConf(1, RetryFailedStrategy.SKIP_EVENT)
        ) {
            `when`(PaymentProcessedEvent::class) { event ->
                val record = PaymentLogRecord(
                    event.processedAt,
                    status = if (event.success) PaymentStatus.SUCCESS else PaymentStatus.FAILED,
                    event.amount,
                    event.paymentId
                )

                // Асинхронная обработка через батчинг
                batchQueue.put(record)
            }
        }
    }

    private fun startBatchProcessor() = Thread({
        val batch = mutableListOf<PaymentLogRecord>()
        while (isRunning) {
            try {
                val record = batchQueue.poll(100, TimeUnit.MILLISECONDS) ?: continue
                batch.add(record)

                if (batch.size >= 1000) {
                    processBatch(batch)
                    batch.clear()
                }
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                break
            }
        }
        // Обработка оставшихся записей
        if (batch.isNotEmpty()) processBatch(batch)
    }, "payment-log-batcher").start()

    private fun processBatch(batch: List<PaymentLogRecord>) {
        if (paymentLog.size >= MAX_LOG_ENTRIES) {
            paymentLog.clear()
            logger.warn("Payment log cleared due to size limit")
        }

        batch.forEach { record ->
            paymentLog
                .computeIfAbsent(record.transactionId) { ConcurrentLinkedQueue() }
                .add(record)
        }
    }

    private fun startCleanupScheduler() = Thread({
        while (isRunning) {
            try {
                Thread.sleep(CLEANUP_INTERVAL_MINUTES * 60 * 1000)
                cleanupOldRecords()
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                break
            }
        }
    }, "payment-log-cleaner").start()

    private fun cleanupOldRecords() {
        val cutoff = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1)
        paymentLog.forEach { (id, records) ->
            records.removeIf { it.timestamp < cutoff }
            if (records.isEmpty()) paymentLog.remove(id)
        }
    }

    @PreDestroy
    fun shutdown() {
        isRunning = false
    }

    // Остальной код класса...
    class PaymentLogRecord(
        val timestamp: Long,
        val status: PaymentStatus,
        val amount: Int,
        val transactionId: UUID,
    )

    enum class PaymentStatus {
        FAILED,
        SUCCESS
    }
}