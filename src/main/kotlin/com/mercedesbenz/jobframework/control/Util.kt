package com.mercedesbenz.jobframework.control

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import org.slf4j.Logger
import kotlin.time.Duration

fun CoroutineScope.scheduleForever(duration: Duration, logger: Logger, task: suspend () -> Unit) {
    launch {
        while (true) {
            supervisorScope {// don't let Exceptions in child threads be propagated upwards
                launch {
                    try {
                        task()
                    } catch (t: Throwable) {
                        logger.error("Caught exception in scheduled task: ${t.message}", t)
                    }
                }
            }
            delay(duration)
        }
    }
}

fun unreachable(): Nothing {
    throw UnreachableCodeException()
}

private class UnreachableCodeException : Throwable("Expected this code to be unreachable")
