// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.control

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.TimeSource.Monotonic.markNow

/**
 * Executes the given [task] repeatedly until this [CoroutineContext] or the returned job is
 * cancelled. If [preventParallelExecutions] is `false` (default) the task will be started
 * independently of the duration of the task every [interval]. In case of `true` the next
 * start of the task will be delayed until the previous task finished.
 *
 * The [taskName] can be an arbitrary (including, but not recommended) the empty string,
 * and is used to provide proper names for the created coroutines.
 *
 * Exceptions in the task will not affect the application or other runs of the task.
 *
 * The default dispatcher used for executing the [task] can be overridden with the given
 * [dispatcher].
 */
@OptIn(ExperimentalTime::class)
fun CoroutineContext.scheduleForever(
    interval: Duration,
    taskName: String,
    preventParallelExecutions: Boolean = false,
    dispatcher: CoroutineDispatcher? = null,
    task: suspend () -> Unit
): Job {
    // the SupervisorJob prevents exceptions from task to cancel the parent coroutine (which might be the whole application)
    val supervisor = SupervisorJob(get(Job))
    val coroutineName = CoroutineName("Single execution of '$taskName'")
    CoroutineScope(this + supervisor).launch(CoroutineName("Continuous Scheduler for '$taskName'")) {
        val context = if (dispatcher != null) dispatcher + supervisor + coroutineName else supervisor + coroutineName
        while (true) {
            if (preventParallelExecutions) {
                val taskStart = markNow()
                task()
                delay(interval - taskStart.elapsedNow())
            } else {
                launch(context) { task() }
                delay(interval)
            }
        }
    }
    return supervisor
}
