// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.executor

import com.booleworks.kjobs.api.JobFramework
import com.booleworks.kjobs.api.persistence.hashmap.HashMapDataPersistence
import com.booleworks.kjobs.api.persistence.hashmap.HashMapJobPersistence
import com.booleworks.kjobs.common.Either
import com.booleworks.kjobs.common.TestInput
import com.booleworks.kjobs.common.TestResult
import com.booleworks.kjobs.common.defaultComputation
import com.booleworks.kjobs.common.defaultInstanceName
import com.booleworks.kjobs.control.ComputationResult
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.equals.shouldBeEqual
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.server.application.call
import io.ktor.server.request.receiveText
import io.ktor.server.response.respond
import io.ktor.server.routing.application
import io.ktor.server.routing.route
import io.ktor.server.testing.testApplication
import kotlinx.coroutines.delay
import kotlin.time.Duration.Companion.milliseconds

class CancellationTest : FunSpec({
    test("test cancellation") {
        val jobPersistence = HashMapJobPersistence()
        val dataPersistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
        testApplication {
            routing {
                route("test") {
                    JobFramework(defaultInstanceName, jobPersistence, Either.Right(application)) {
                        addApi(
                            "J1",
                            this@route,
                            dataPersistence,
                            { TestInput(call.receiveText().toInt(), 100_000) },
                            { call.respond(it.inputValue) },
                            defaultComputation
                        )
                        cancellationConfig {
                            enabled = true
                            checkInterval = 5.milliseconds
                        }
                        maintenanceConfig {
                            jobCheckInterval = 5.milliseconds
                        }
                    }
                }
            }
            val uuid = client.post("test/submit") { setBody("5") }.bodyAsText()
            delay(20)
            client.get("test/status/$uuid").bodyAsText() shouldBeEqual "RUNNING"
            client.post("test/cancel/$uuid")
                .bodyAsText() shouldBeEqual "Job with id $uuid is currently running and will be cancelled as soon as possible. If it finishes in the meantime, the cancel request will be ignored."
            delay(20)
            client.get("test/status/$uuid").bodyAsText() shouldBeEqual "CANCELLED"
        }
    }

    test("test cancellation with blocking computation") {
        val jobPersistence = HashMapJobPersistence()
        val dataPersistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
        testApplication {
            routing {
                route("test") {
                    JobFramework(defaultInstanceName, jobPersistence, Either.Right(application)) {
                        addApi(
                            "J1",
                            this@route,
                            dataPersistence,
                            { TestInput(call.receiveText().toInt(), 100_000) },
                            { call.respond(it.inputValue) },
                            { _, input -> Thread.sleep(input.expectedDelay.toLong()); ComputationResult.Success(TestResult(input.value)) }
                        )
                        cancellationConfig {
                            enabled = true
                            checkInterval = 5.milliseconds
                        }
                        maintenanceConfig {
                            jobCheckInterval = 5.milliseconds
                        }
                    }
                }
            }
            val uuid = client.post("test/submit") { setBody("5") }.bodyAsText()
            delay(20)
            client.get("test/status/$uuid").bodyAsText() shouldBeEqual "RUNNING"
            client.post("test/cancel/$uuid")
                .bodyAsText() shouldBeEqual "Job with id $uuid is currently running and will be cancelled as soon as possible. If it finishes in the meantime, the cancel request will be ignored."
            delay(500)
            client.get("test/status/$uuid").bodyAsText() shouldBeEqual "CANCEL_REQUESTED"
        }
    }
})
