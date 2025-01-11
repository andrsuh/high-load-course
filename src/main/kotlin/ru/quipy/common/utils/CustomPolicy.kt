package ru.quipy.common.utils

import org.slf4j.LoggerFactory
import java.util.concurrent.RejectedExecutionHandler
import java.util.concurrent.ThreadPoolExecutor

class CustomPolicy : RejectedExecutionHandler {
    companion object {
        val logger = LoggerFactory.getLogger(CustomPolicy::class.java)
    }

    override fun rejectedExecution(r: Runnable, e: ThreadPoolExecutor) {
        logger.error("Task was rejected from payment executor: $r")
    }
}