// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs

import com.booleworks.kjobs.control.scheduleForever
import io.kotest.assertions.fail
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.ints.shouldBeGreaterThan
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds

class ScheduleForeverTest : FunSpec({
    test("test exceptions not propagated") {
        runCatching {
            runBlocking {
                val i = AtomicInteger(0)
                val job = coroutineContext.scheduleForever(100.milliseconds) {
                    i.incrementAndGet()
                    error("Test -- should not be propagated")
                }
                delay(500.milliseconds)
                i.get() shouldBeGreaterThan 2
                job.cancel()
            }
        }.onFailure { fail("No exception expected") }
    }
})
