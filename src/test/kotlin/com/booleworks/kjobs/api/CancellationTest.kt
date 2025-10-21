// SPDX-License-Identifier: MIT
// Copyright 2023-2024 BooleWorks GmbH

package com.booleworks.kjobs.api

import com.booleworks.kjobs.common.Either
import com.booleworks.kjobs.common.TestInput
import com.booleworks.kjobs.common.TestResult
import com.booleworks.kjobs.common.defaultComputation
import com.booleworks.kjobs.common.defaultInstanceName
import com.booleworks.kjobs.common.expectSuccess
import com.booleworks.kjobs.common.newRedisPersistence
import com.booleworks.kjobs.common.reset
import com.booleworks.kjobs.common.setRunning
import com.booleworks.kjobs.common.testJobFrameworkWithRedis
import com.booleworks.kjobs.control.ComputationResult
import com.booleworks.kjobs.data.JobStatus
import com.github.fppt.jedismock.RedisServer
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.equals.shouldBeEqual
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.server.application.call
import io.ktor.server.request.receiveText
import io.ktor.server.response.respond
import io.ktor.server.routing.route
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlin.time.Duration.Companion.milliseconds

class CancellationTest : FunSpec({
    testJobFrameworkWithRedis("test cancellation") {
        val persistence = newRedisPersistence<TestInput, TestResult>()
        var frameworkJob: kotlinx.coroutines.Job? = null
        routing {
            route("test") {
                frameworkJob = JobFramework(defaultInstanceName, persistence) {
                    addApi(
                        "J1", this@route, persistence, { TestInput(receiveText().toInt(), 100_000) },
                        { respond(it.inputValue) }, defaultComputation
                    )
                    enableCancellation {
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
        delay(200)
        client.get("test/status/$uuid").bodyAsText() shouldBeEqual "CANCELLED"
        frameworkJob!!.cancelAndJoin()
    }

    testJobFrameworkWithRedis("test cancellation with blocking computation") {
        val persistence = newRedisPersistence<TestInput, TestResult>()
        var frameworkJob: kotlinx.coroutines.Job? = null
        routing {
            route("test") {
                frameworkJob = JobFramework(defaultInstanceName, persistence) {
                    addApi(
                        "J1",
                        this@route,
                        persistence,
                        { TestInput(receiveText().toInt(), 2000) },
                        { respond(it.inputValue) },
                        { _, input -> Thread.sleep(input.expectedDelay.toLong()); ComputationResult.Success(TestResult(input.value)) }
                    )
                    enableCancellation {
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
        frameworkJob!!.cancelAndJoin()
    }

    test("test cancellation from other states") {
        val redis = RedisServer.newRedisServer().start()
        val persistence = newRedisPersistence<TestInput, TestResult>(redis)
        var frameworkJob: kotlinx.coroutines.Job? = null
        val testingMode = JobFrameworkTestingMode("ME", persistence, false) {
            addJob("J1", persistence, { _, input: TestInput -> ComputationResult.Success(TestResult(input.value)) })
        }

        val job = testingMode.submitJob("J1", TestInput(42)).expectSuccess()
        val uuid = job.uuid
        persistence.fetchJob(uuid).expectSuccess().status shouldBeEqual JobStatus.CREATED
        testingMode.cancelJob(persistence.fetchJob(uuid).expectSuccess()) shouldBeEqual Either.Right("Job with id $uuid was cancelled successfully")
        persistence.fetchJob(uuid).expectSuccess().status shouldBeEqual JobStatus.CANCELLED

        testingMode.cancelJob(persistence.fetchJob(uuid).expectSuccess()) shouldBeEqual Either.Right("Job with id $uuid has already finished with status CANCELLED")
        persistence.fetchJob(uuid).expectSuccess().status shouldBeEqual JobStatus.CANCELLED

        setRunning(persistence, testingMode, uuid)
        testingMode.cancelJob(persistence.fetchJob(uuid).expectSuccess()) shouldBeEqual Either.Right(
            "Job with id $uuid is currently running and will be cancelled as soon as possible. " +
                    "If it finishes in the meantime, the cancel request will be ignored."
        )
        persistence.fetchJob(uuid).expectSuccess().status shouldBeEqual JobStatus.CANCEL_REQUESTED

        testingMode.cancelJob(persistence.fetchJob(uuid).expectSuccess()) shouldBeEqual Either.Right("Cancellation for job with id $uuid has already been requested")
        persistence.fetchJob(uuid).expectSuccess().status shouldBeEqual JobStatus.CANCEL_REQUESTED

        reset(persistence, testingMode, uuid)
        testingMode.runExecutor()
        persistence.fetchJob(uuid).expectSuccess().status shouldBeEqual JobStatus.SUCCESS
        testingMode.cancelJob(persistence.fetchJob(uuid).expectSuccess()) shouldBeEqual Either.Right("Job with id $uuid has already finished with status SUCCESS")
        persistence.fetchJob(uuid).expectSuccess().status shouldBeEqual JobStatus.SUCCESS

        job.status = JobStatus.FAILURE
        persistence.transaction { updateJob(job) }.expectSuccess()
        testingMode.cancelJob(job) shouldBeEqual Either.Right("Job with id $uuid has already finished with status FAILURE")
        persistence.fetchJob(uuid).expectSuccess().status shouldBeEqual JobStatus.FAILURE

        redis.stop()
    }
})
