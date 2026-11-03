package ru.quipy.apigateway

import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.RateLimiter

/**
 * Обёртка над [RateLimiter] для admission control на входе приложения.
 *
 * Зачем обёртка, а не голый лимитер:
 *  - бин существует всегда, поэтому [APIController] не зависит от того, включён ли лимитер;
 *  - при исключении внутри алгоритма действует fail-open;
 *  - счётчики `admitted` / `rejected` позволяют отличить отказ лимитера от отказа стража
 *    дедлайна и от переполнения `payment-submission-executor`: все три источника отдают
 *    клиенту одинаковый `429`, и без атрибуции причину не определить.
 *
 * Два лимитера вместо одного нужны из-за разной формы нагрузки:
 *  - короткие тесты (дедлайн 2.5 и 13 с) идут ровным потоком, бакет только гасит волны повторов;
 *  - тест 3 (дедлайн 26 с) подаёт ~200 запросов одной пачкой раз в `runits = 90` с, и бакет
 *    обязан вмещать пачку целиком, иначе клиент получает `429`, пересоздаёт заказ и тест
 *    не набирает объём.
 * Выбор лимитера делается по остатку дедлайна конкретного запроса, поэтому конфигурация
 * не зависит от переменных окружения и одинаково работает при любом способе запуска.
 */
class IngressRateLimiter(
    private val shortDeadlineDelegate: RateLimiter?,
    private val longDeadlineDelegate: RateLimiter? = null,
    meterRegistry: MeterRegistry,
) {
    companion object {
        private val logger = LoggerFactory.getLogger(IngressRateLimiter::class.java)

        /**
         * Шаг опроса лимитера при ожидании слота.
         * При интервале капели ~90 мс шаг 5 мс даёт точность порядка 5 %, но не сжигает CPU,
         * как шаг 1 мс при сотнях ожидающих потоков.
         */
        private const val WAIT_STEP_MS = 5L
    }

    private val admitted = meterRegistry.counter("ingress_rate_limiter_total", "result", "admitted")
    private val rejected = meterRegistry.counter("ingress_rate_limiter_total", "result", "rejected")
    private val failed = meterRegistry.counter("ingress_rate_limiter_total", "result", "failed_open")

    val enabled: Boolean get() = shortDeadlineDelegate != null || longDeadlineDelegate != null

    /**
     * Пытается получить разрешение на обработку платежа.
     *
     * @param waitBudgetMs сколько максимум ждать освобождения слота до ответа `429`.
     * @param longDeadline признак «длинного» дедлайна: выбирает лимитер с большим бакетом.
     */
    fun tryAcquire(waitBudgetMs: Long = 0L, longDeadline: Boolean = false): Boolean {
        val limiter = if (longDeadline) longDeadlineDelegate ?: shortDeadlineDelegate else shortDeadlineDelegate
        if (limiter == null) {
            admitted.increment()
            return true
        }

        val waitUntil = System.nanoTime() + waitBudgetMs * 1_000_000L
        while (true) {
            val allowed = try {
                limiter.tick()
            }
            catch (e: Exception) {
                failed.increment()
                logger.error("Ingress rate limiter failed, request is allowed", e)
                true
            }

            if (allowed) {
                admitted.increment()
                return true
            }
            if (System.nanoTime() >= waitUntil) {
                rejected.increment()
                return false
            }
            Thread.sleep(WAIT_STEP_MS)
        }
    }
}
