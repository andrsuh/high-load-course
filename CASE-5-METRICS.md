# Кейс 5: Метрики для мониторинга

## Реализованные метрики

### 1. Counter метрики (бесконечно растущие счетчики)

#### `payment_submissions_total{account="..."}`
- **Назначение**: Общее количество отправленных запросов на оплату
- **Тип**: Counter
- **Labels**: account (название аккаунта)
- **Использование**: Подсчет всех submission событий

#### `payment_success_total{account="..."}`
- **Назначение**: Количество успешных платежей
- **Тип**: Counter
- **Labels**: account
- **Использование**: Подсчет успешных обработок

#### `payment_failures_total{account="...", reason="..."}`
- **Назначение**: Количество неуспешных платежей
- **Тип**: Counter
- **Labels**: account, reason (причина ошибки)
- **Использование**: Подсчет ошибок с детализацией по причинам

#### `payment_retries_total{account="...", status_code="..."}`
- **Назначение**: Количество повторных попыток (retry)
- **Тип**: Counter
- **Labels**: account, status_code (HTTP код или 0 для timeout)
- **Использование**: Отслеживание retry для анализа проблем

#### `payment_timeouts_total{account="...", type="..."}`
- **Назначение**: Количество таймаутов
- **Тип**: Counter
- **Labels**: account, type (тип таймаута)
- **Использование**: Отслеживание различных типов таймаутов

### 2. Gauge метрики (моментальные значения)

#### `payment_queue_size{account="..."}`
- **Назначение**: Текущий размер очереди задач в ThreadPoolExecutor
- **Тип**: Gauge
- **Labels**: account
- **Использование**: Мониторинг заполненности очереди

#### `payment_active_threads{account="..."}`
- **Назначение**: Количество активных потоков в пуле
- **Тип**: Gauge
- **Labels**: account
- **Использование**: Отслеживание загрузки пула потоков

#### `payment_semaphore_available{account="..."}`
- **Назначение**: Доступные разрешения в семафоре
- **Тип**: Gauge
- **Labels**: account
- **Использование**: Мониторинг доступности параллельных слотов

### 3. Summary метрики (с квантилями)

#### `payment_request_duration_ms{account="...", quantile="..."}`
- **Назначение**: Время выполнения запроса в миллисекундах
- **Тип**: DistributionSummary (аналог Summary)
- **Labels**: account
- **Квантили**: 0.5 (медиана), 0.85, 0.95, 0.99
- **Дополнительно**: _sum и _count для вычисления среднего
- **Использование**: Анализ времени выполнения, выявление медленных запросов

#### `payment_queue_wait_time_ms{account="...", quantile="..."}`
- **Назначение**: Время ожидания в очереди перед обработкой
- **Тип**: DistributionSummary
- **Labels**: account
- **Квантили**: 0.5, 0.85, 0.95, 0.99
- **Использование**: Анализ задержек в очереди

## Как проверить метрики

### 1. Запустить приложение
```bash
export PAYMENT_ACCOUNTS=acc-23 && mvn spring-boot:run
```

### 2. Просмотреть метрики
```bash
curl http://localhost:8081/actuator/prometheus | grep payment_
```

### 3. Примеры полезных запросов в Prometheus/Grafana

#### Среднее время выполнения запросов (скользящее окно 1 минута)
```promql
rate(payment_request_duration_ms_sum{account="acc-23"}[1m]) /
rate(payment_request_duration_ms_count{account="acc-23"}[1m])
```

#### Медиана времени выполнения
```promql
payment_request_duration_ms{account="acc-23", quantile="0.5"}
```

#### 95-й перцентиль времени выполнения
```promql
payment_request_duration_ms{account="acc-23", quantile="0.95"}
```

#### Rate успешных платежей в секунду
```promql
rate(payment_success_total{account="acc-23"}[1m])
```

#### Rate ошибок в секунду
```promql
rate(payment_failures_total{account="acc-23"}[1m])
```

#### Процент успешных запросов
```promql
rate(payment_success_total{account="acc-23"}[1m]) /
(rate(payment_success_total{account="acc-23"}[1m]) +
 rate(payment_failures_total{account="acc-23"}[1m])) * 100
```

#### Текущий размер очереди
```promql
payment_queue_size{account="acc-23"}
```

#### Доступные слоты семафора
```promql
payment_semaphore_available{account="acc-23"}
```

#### Количество retry по типам
```promql
sum by (status_code) (rate(payment_retries_total{account="acc-23"}[1m]))
```

## Соответствие теории из high-load-systems-theory.md

### Кейс 5: Буферизация запросов и мониторинг

✅ **Counter метрики** - реализовано:
- Подсчет всех событий (submissions, success, failures)
- Функции rate/increase для анализа

✅ **Gauge метрики** - реализовано:
- Размер очередей (queue_size)
- Активные потоки (active_threads)
- Доступные разрешения (semaphore_available)

✅ **Summary метрики** - реализовано (кейс 7):
- Квантили для анализа времени выполнения (0.5, 0.85, 0.95, 0.99)
- Sum/count для вычисления среднего
- Медиана как альтернатива среднему арифметическому

## Дашборд для Grafana

Рекомендуемые панели:

1. **Request Rate**: `rate(payment_submissions_total[1m])`
2. **Success Rate**: `rate(payment_success_total[1m])`
3. **Error Rate**: `rate(payment_failures_total[1m])`
4. **Queue Size**: `payment_queue_size`
5. **Active Threads**: `payment_active_threads`
6. **Latency (p50, p95, p99)**: `payment_request_duration_ms`
7. **Queue Wait Time**: `payment_queue_wait_time_ms`
