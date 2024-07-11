// SPDX-License-Identifier: MIT
// Copyright 2023-2024 BooleWorks GmbH

package com.booleworks.kjobs

import com.booleworks.kjobs.control.scheduleForever
import io.kotest.assertions.fail
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.ints.shouldBeGreaterThan
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds

class ScheduleForeverTest : FunSpec({
    test("test exceptions not propagated") {
        runCatching {
            runBlocking {
                val i = AtomicInteger(0)
                val job = coroutineContext.scheduleForever(100.milliseconds, "test", true) {
                    i.incrementAndGet()
                    error("Test -- should not be propagated")
                }
                delay(500.milliseconds)
                i.get() shouldBeGreaterThan 2
                job.cancelAndJoin()
            }
        }.onFailure { fail("No exception expected") }
    }

    test("test prevent parallel execution") {
        runCatching {
            runBlocking {
                val i = AtomicInteger(0)
                val job = coroutineContext.scheduleForever(100.milliseconds, "test", preventParallelExecutions = false) {
                    i.incrementAndGet()
                    delay(1000.milliseconds)
                }
                delay(500.milliseconds)
                i.get() shouldBeGreaterThan 2
                job.cancelAndJoin()
            }
            runBlocking {
                val i = AtomicInteger(0)
                val job = coroutineContext.scheduleForever(100.milliseconds, "test", preventParallelExecutions = true) {
                    i.incrementAndGet()
                    delay(1000.milliseconds)
                }
                delay(500.milliseconds)
                i.get() shouldBe 1
                job.cancelAndJoin()
            }
            runBlocking {
                val i = AtomicInteger(0)
                val job = coroutineContext.scheduleForever(100.milliseconds, "test", preventParallelExecutions = true) {
                    i.incrementAndGet()
                    delay(500.milliseconds)
                }
                delay(800.milliseconds)
                i.get() shouldBe 2
                job.cancelAndJoin()
            }
        }.onFailure { fail("No exception expected") }
    }
})
