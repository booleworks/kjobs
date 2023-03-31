package com.mercedesbenz.jobframework.control

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlin.time.Duration

fun CoroutineScope.scheduleForever(duration: Duration, task: suspend () -> Unit) {
    launch {
        while (true) {
            launch {
                // this should not be a child of the parent scope -- if it fails, we do not want it to destroy the parent
                // TODO do we want to catch and log any exception thrown in task?
                runBlocking { task() }
            }
            delay(duration)
        }
    }
}

fun unreachable(): Nothing {
    throw UnreachableCodeException()
}

private class UnreachableCodeException : Throwable("Expected this code to be unreachable")
