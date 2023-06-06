// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.control

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration

/**
 * Executes the given [task] repeatedly until this [CoroutineContext] or the returned job is
 * cancelled. The task will be started every [interval], independently of the duration of
 * the task.
 *
 * Exceptions in the task will not affect the application or other runs of the task.
 *
 * By default, the task will be launched in a [SupervisorJob] of this context, but it can be
 * overridden (e.g. for long-running tasks) using a custom [dispatcher].
 */
fun CoroutineContext.scheduleForever(interval: Duration, dispatcher: CoroutineDispatcher? = null, task: suspend () -> Unit): Job {
    // the SupervisorJob prevents exceptions from task to cancel the parent coroutine (which might be the whole application)
    val job = SupervisorJob(get(Job))
    CoroutineScope(job).launch {
        while (true) {
            if (dispatcher != null)
                launch(dispatcher) { task() }
            else
                launch { task() }
            delay(interval)
        }
    }
    return job
}
