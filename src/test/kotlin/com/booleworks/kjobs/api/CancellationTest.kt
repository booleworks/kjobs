// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.api

import com.booleworks.kjobs.api.persistence.hashmap.HashMapDataPersistence
import com.booleworks.kjobs.api.persistence.hashmap.HashMapJobPersistence
import com.booleworks.kjobs.common.TestInput
import com.booleworks.kjobs.common.TestResult
import com.booleworks.kjobs.common.defaultComputation
import com.booleworks.kjobs.common.defaultInstanceName
import com.booleworks.kjobs.common.expectSuccess
import com.booleworks.kjobs.common.reset
import com.booleworks.kjobs.common.setRunning
import com.booleworks.kjobs.control.ComputationResult
import com.booleworks.kjobs.data.JobStatus
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
                    JobFramework(defaultInstanceName, jobPersistence, application) {
                        addApi(
                            "J1", this@route, dataPersistence, { TestInput(call.receiveText().toInt(), 100_000) },
                            { call.respond(it.inputValue) }, defaultComputation
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
                    JobFramework(defaultInstanceName, jobPersistence, application) {
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

    test("test cancellation from other states") {
        val jobPersistence = HashMapJobPersistence()
        val dataPersistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
        val testingMode = JobFrameworkTestingMode("ME", jobPersistence, this, false) {
            addJob("J1", dataPersistence, { _, input: TestInput -> ComputationResult.Success(TestResult(input.value)) })
        }

        val job = testingMode.submitJob("J1", TestInput(42)).expectSuccess()
        jobPersistence.fetchJob(job.uuid).expectSuccess().status shouldBeEqual JobStatus.CREATED
        testingMode.cancelJob(job) shouldBeEqual "Job with id ${job.uuid} was cancelled successfully"
        jobPersistence.fetchJob(job.uuid).expectSuccess().status shouldBeEqual JobStatus.CANCELLED

        testingMode.cancelJob(job) shouldBeEqual "Job with id ${job.uuid} has already finished with status CANCELLED"
        jobPersistence.fetchJob(job.uuid).expectSuccess().status shouldBeEqual JobStatus.CANCELLED

        job.setRunning(jobPersistence)
        testingMode.cancelJob(job) shouldBeEqual "Job with id ${job.uuid} is currently running and will be cancelled as soon as possible. " +
                "If it finishes in the meantime, the cancel request will be ignored."
        jobPersistence.fetchJob(job.uuid).expectSuccess().status shouldBeEqual JobStatus.CANCEL_REQUESTED

        testingMode.cancelJob(job) shouldBeEqual "Cancellation for job with id ${job.uuid} has already been requested"
        jobPersistence.fetchJob(job.uuid).expectSuccess().status shouldBeEqual JobStatus.CANCEL_REQUESTED

        job.reset(jobPersistence)
        testingMode.runExecutor()
        jobPersistence.fetchJob(job.uuid).expectSuccess().status shouldBeEqual JobStatus.SUCCESS
        testingMode.cancelJob(job) shouldBeEqual "Job with id ${job.uuid} has already finished with status SUCCESS"
        jobPersistence.fetchJob(job.uuid).expectSuccess().status shouldBeEqual JobStatus.SUCCESS

        job.status = JobStatus.FAILURE
        jobPersistence.updateJob(job)
        testingMode.cancelJob(job) shouldBeEqual "Job with id ${job.uuid} has already finished with status FAILURE"
        jobPersistence.fetchJob(job.uuid).expectSuccess().status shouldBeEqual JobStatus.FAILURE
    }
})
