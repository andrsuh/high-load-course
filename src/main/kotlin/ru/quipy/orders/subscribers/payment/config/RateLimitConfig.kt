package ru.quipy.orders.subscribers.payment.config

import java.time.Duration

class RateLimitConfig(val rate: Long, val window: Duration) {}