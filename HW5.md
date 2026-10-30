# HW5: Входной Rate Limiting

## Решение

### Идея

В предыдущих работах (hw-02..hw-04) использовался **выходной rate limiting** — ограничение потока запросов к провайдеру.
Для HW5 реализован **входной rate limiting** на уровне API Gateway, который контролирует поток запросов до попадания в
очередь обработки.

**Ключевые компоненты:**

1. **IngressRateLimiterConfig** — конфигурация входного rate limiter'а с поддержкой различных алгоритмов:
    - `sliding` — SlidingWindowRateLimiter (по умолчанию)
    - `fixed` — FixedWindowRateLimiter
    - `token-bucket` — TokenBucketRateLimiter с burst capacity
    - `leaking-bucket` — LeakingBucketRateLimiter
    - `counting` — CountingRateLimiter
    - `composite-burst` — комбинация token bucket + sliding window

2. **APIController** — модифицирован для проверки rate limit перед обработкой платежа:
    - Проверка `ingressRateLimiter.tick()` до вызова `orderPayer.processPayment()`
    - HTTP 429 с `Retry-After` заголовком при превышении лимита
    - Конфигурируемое значение `retryAfterMs`

3. **application.properties** — параметры для настройки:
   ```properties
   ingress.rate-limiter.enabled=true
   ingress.rate-limiter.type=sliding
   ingress.rate-limiter.rate=11
   ingress.rate-limiter.window-seconds=1
   ingress.retry-after-ms=500
   ```

## Карточка аккаунта:

```json
{
  "serviceName": "cas-m3404-03",
  "accountName": "acc-23",
  "parallelRequests": 64,
  "rateLimitPerSec": 11,
  "price": 30,
  "averageProcessingTime": "PT1S",
  "enabled": true
}
```

## Тест 1

### Условия:

```json
{
  "Аккаунт": "acc-23",
  "ratePerSecond": 15,
  "testCount": 3000,
  "processingTimeMillis": 2500
}
```

## Тест 2

### Условия:

```json
{
  "Аккаунт": "acc-23",
  "ratePerSecond": 11,
  "testCount": 2200,
  "processingTimeMillis": 13000,
  "profile": "s_0.7_60"
}
```

## Тест 3

### Условия:

```json
{
  "Аккаунт": "acc-23",
  "ratePerSecond": 3,
  "testCount": 1050,
  "processingTimeMillis": 26000,
  "runits": 90
}
```

## Ориентиры и расчётные параметры

`acc-23`: `parallelRequests = 64`, `rateLimitPerSec = 11`, `price = 30`, `averageProcessingTime = PT1S`.

Потолок пропускной способности: `rps_max = parallelRequests / processingTimeSec`.
Необходимый параллелизм для 100% утилизации: `inFlight = rateLimitPerSec × processingTimeSec`.
Стоимость попытки: `expenses ≈ (Success + Fail) × price`.

| Тест | Вход rps | testCount | processing (мс) | Особые параметры    | inFlight при 100% | Потолок при 64 слотах            | Ориентир: время     | Fail / Success | Income / Expenses |
|------|----------|-----------|-----------------|---------------------|-------------------|----------------------------------|---------------------|----------------|-------------------|
| 1    | 15       | 3000      | 2500            | —                   | 11 × 2.5 ≈ 28     | 64 / 2.5 ≈ 25.6 rps (ок)         | ≈ 273 с (3000 / 11) | —              | —                 |
| 2    | 11       | 2200      | 13000           | `profile: s_0.7_60` | 11 × 13 = 143     | 64 / 13 ≈ 4.9 rps (**дефицит**)  | 3.5 мин             | 42 / 2230      | 892k / 68k        |
| 3    | 3        | 1050      | 26000           | `runits: 90`        | 3 × 26 = 78       | 64 / 26 ≈ 2.46 rps (**дефицит**) | 6 мин               | 2 / 1060       | 428k / 31.9k      |

Контрольные точки для сверки: `2272 × 30 =	68	160 ≈	68k` (тест 2), `1062 ×	30 =	31	860 ≈	31.9k` (
тест 3).

## Ритуал прогона

1. `POST http://localhost:1234/test/stop/{{serviceName}}` — убедиться, что предыдущий прогон остановлен, пауза 2–3 с.
2. При необходимости перезапустить приложение (сброс счётчиков Micrometer) и `docker compose restart bombardier` (сброс
   `external_sys_*` / `http_external_*`).
3. Проверить, что таргет `online-store-job` = UP в `http://localhost:9090/targets`.
4. Записать в журнал: дату/время старта (UTC), конфигурацию теста и конфигурацию лимитеров.
5. Запустить `POST http://localhost:1234/test/run` с `# @timeout 600` (иначе HTTP-клиент оборвёт соединение на 120 с, а
   тест продолжится).
6. Во время прогона контролировать: `executor_active_threads` (утилизация), `executor_queued_tasks` (back pressure),
   строки `Submit:` в логе приложения.
7. После завершения записать время окончания и итоги из ответа раннера, снять скриншот в Grafana в **абсолютном**
   диапазоне `[старт − 1 мин; конец + 1 мин]`.
8. Между прогонами выдерживать паузу не менее 90 с, чтобы окна `rate[30s]` и `increase[1m]` не смешивали прогоны.

## Журнал прогонов

Заполняется после каждого прогона. Для скриншотов обязателен абсолютный диапазон времени, иначе значения
`increase(...[$__range])` суммируют соседние прогоны.

| # | Тест   | Дата, время UTC (start → end) | Лимитеры (вход / выход) | Длительность | Fail / Success | Income / Expenses | max rps (факт) | 429 / reject | max queued | Скриншот | Вывод |
|---|--------|-------------------------------|-------------------------|--------------|----------------|-------------------|----------------|--------------|------------|----------|-------|
| 1 | test-1 | 22:58 -> 23:02                |                         |              |                |                   |                |              |            |          |       |
| 2 | test-2 |                               |                         |              |                |                   |                |              |            |          |       |
| 3 | test-3 |                               |                         |              |                |                   |                |              |            |          |       |
