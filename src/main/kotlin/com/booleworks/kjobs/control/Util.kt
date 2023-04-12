// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.control

import io.ktor.server.application.Application
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlin.time.Duration

/**
 * Executes the given task with an interval of [duration]. The task will be executed until the application stops.
 * Also, the schedule can be stopped by invoking [Job.cancel] on the returned job.
 * Exceptions in the task will not affect the application or other runs of the task.
 */
fun Application.scheduleForever(duration: Duration, task: suspend () -> Unit): Job = launch(coroutineContext) {
    scheduleForever(duration, task)
}

/**
 * Executes the given task with an interval of [duration].
 * Prefer [Application.scheduleForever] if you want to schedule a task from a ktor application.
 */
fun CoroutineScope.scheduleForever(duration: Duration, task: suspend () -> Unit): Job {
    // the SupervisorJob prevents exceptions from task to cancel the parent coroutine (which might be the whole application)
    val job = SupervisorJob(coroutineContext.job)
    launch {
        while (true) {
            launch(job) { task() }
            delay(duration)
        }
    }
    return job
}

fun unreachable(): Nothing {
    throw UnreachableCodeException()
}

private class UnreachableCodeException : Throwable("Expected this code to be unreachable")
