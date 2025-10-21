// SPDX-License-Identifier: MIT
// Copyright 2023-2024 BooleWorks GmbH

package com.booleworks.kjobs.api

import com.booleworks.kjobs.api.persistence.hashmap.HashMapDataPersistence
import com.booleworks.kjobs.api.persistence.hashmap.HashMapJobPersistence
import com.booleworks.kjobs.common.TestInput
import com.booleworks.kjobs.common.TestResult
import com.booleworks.kjobs.common.defaultComputation
import com.booleworks.kjobs.common.defaultInstanceName
import com.booleworks.kjobs.common.expectSuccess
import com.booleworks.kjobs.common.parseTestResult
import com.booleworks.kjobs.common.right
import com.booleworks.kjobs.data.JobStatus
import com.booleworks.kjobs.data.PersistenceAccessResult
import com.booleworks.kjobs.data.result
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.equals.shouldBeEqual
import io.kotest.matchers.string.shouldStartWith
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.call
import io.ktor.server.request.receiveText
import io.ktor.server.response.respond
import io.ktor.server.routing.route
import io.ktor.server.testing.testApplication
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlin.time.Duration.Companion.milliseconds

class SyncMockTest : FunSpec({

    test("test simple synchronous api") {
        val jobPersistence = HashMapJobPersistence()
        val dataPersistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
        var jobFramework: kotlinx.coroutines.Job? = null
        testApplication {
            routing {
                route("test") {
                    jobFramework = JobFramework(defaultInstanceName, jobPersistence) {
                        addApi(
                            "J1", this@route, dataPersistence, { TestInput(call.receiveText().toInt()) },
                            { call.respond(it) }, defaultComputation
                        ) {
                            enableSynchronousResource { checkInterval = 5.milliseconds }
                        }
                        maintenanceConfig { jobCheckInterval = 5.milliseconds }
                    }
                }
            }
            client.post("test/synchronous") { setBody("42") }.parseTestResult() shouldBeEqual TestResult(42)
            client.post("test/synchronous") { setBody("43") }.parseTestResult() shouldBeEqual TestResult(43)
        }
        dataPersistence.fetchAllJobs().right() shouldHaveSize 2 // jobs are kept by default
        jobFramework!!.cancelAndJoin()
    }

    test("test synchronous config options") {
        val jobPersistence = HashMapJobPersistence()
        val dataPersistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
        var jobFramework: kotlinx.coroutines.Job? = null
        testApplication {
            routing {
                route("test") {
                    jobFramework = JobFramework(defaultInstanceName, jobPersistence) {
                        addApi(
                            "J1", this@route, dataPersistence, { call.receiveText().toInt().let { TestInput(it, it) } },
                            { call.respond(it) }, defaultComputation
                        ) {
                            enableSynchronousResource {
                                checkInterval = 5.milliseconds
                                maxWaitingTime = 50.milliseconds
                                customPriorityProvider = { it.value }
                            }
                            jobConfig { priorityProvider = { 2 } }
                        }
                        maintenanceConfig { jobCheckInterval = 5.milliseconds }
                    }
                }
            }
            client.post("test/synchronous") { setBody("20") }.parseTestResult() shouldBeEqual TestResult(20)
            val jobs = jobPersistence.allJobsOfInstance(JobStatus.SUCCESS, defaultInstanceName).expectSuccess() shouldHaveSize 1
            jobs.first().priority shouldBeEqual 20

            val abortedJob = client.post("test/synchronous") { setBody("100") }
            abortedJob.status shouldBeEqual HttpStatusCode.BadRequest
            val response = abortedJob.bodyAsText()
            response shouldStartWith "The job did not finish within the timeout of 50ms. You may be able to retrieve the result later via the asynchronous API using the job id "

            val uuid = response.split(" ").last().dropLast(1)
            client.get("test/status/$uuid").bodyAsText() shouldBeEqual "RUNNING"
            delay(100)
            client.get("test/status/$uuid").bodyAsText() shouldBeEqual "SUCCESS"
        }
        jobFramework!!.cancelAndJoin()
    }

    test("test failure in synchronous api") {
        val jobPersistence = HashMapJobPersistence()
        val dataPersistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
        var jobFramework: kotlinx.coroutines.Job? = null
        testApplication {
            routing {
                route("test") {
                    jobFramework = JobFramework(defaultInstanceName, jobPersistence) {
                        addApi(
                            "J1", this@route, dataPersistence, { TestInput(call.receiveText().toInt(), throwException = true) },
                            { call.respond(it) }, defaultComputation
                        ) {
                            enableSynchronousResource { checkInterval = 10.milliseconds }
                        }
                        maintenanceConfig { jobCheckInterval = 5.milliseconds }
                    }
                }
            }
            val response = client.post("test/synchronous") { setBody("42") }
            response.status shouldBeEqual HttpStatusCode.InternalServerError
            response.bodyAsText() shouldBeEqual "Unexpected exception during computation: Test Exception Message"
        }
        dataPersistence.fetchAllJobs().right() shouldHaveSize 1 // jobs are kept
        jobFramework!!.cancelAndJoin()
    }

    test("test activated job deletion for successful computation") {
        val jobPersistence = HashMapJobPersistence()
        val dataPersistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
        var jobFramework: kotlinx.coroutines.Job? = null
        testApplication {
            routing {
                route("test") {
                    jobFramework = JobFramework(defaultInstanceName, jobPersistence) {
                        addApi(
                            "J1", this@route, dataPersistence, { TestInput(call.receiveText().toInt()) },
                            { call.respond(it) }, defaultComputation
                        ) {
                            apiConfig {
                                deleteJobAfterFetchingResult = true
                            }
                            enableSynchronousResource { checkInterval = 5.milliseconds }
                        }
                        maintenanceConfig { jobCheckInterval = 5.milliseconds }
                    }
                }
            }
            client.post("test/synchronous") { setBody("42") }.parseTestResult() shouldBeEqual TestResult(42)
            client.post("test/synchronous") { setBody("43") }.parseTestResult() shouldBeEqual TestResult(43)
        }
        dataPersistence.fetchAllJobs() shouldBeEqual PersistenceAccessResult.result(listOf()) // jobs are deleted
        jobFramework!!.cancelAndJoin()
    }

    test("test activated job deletion for failure computation") {
        val jobPersistence = HashMapJobPersistence()
        val dataPersistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
        var jobFramework: kotlinx.coroutines.Job? = null
        testApplication {
            routing {
                route("test") {
                    jobFramework = JobFramework(defaultInstanceName, jobPersistence) {
                        addApi(
                            "J1", this@route, dataPersistence, { TestInput(call.receiveText().toInt(), throwException = true) },
                            { call.respond(it) }, defaultComputation
                        ) {
                            apiConfig {
                                deleteJobAfterFetchingResult = true
                            }
                            enableSynchronousResource { checkInterval = 10.milliseconds }
                        }
                        maintenanceConfig { jobCheckInterval = 5.milliseconds }
                    }
                }
            }
            val response = client.post("test/synchronous") { setBody("42") }
            response.status shouldBeEqual HttpStatusCode.InternalServerError
            response.bodyAsText() shouldBeEqual "Unexpected exception during computation: Test Exception Message"
        }
        dataPersistence.fetchAllJobs() shouldBeEqual PersistenceAccessResult.result(listOf()) // jobs are deleted
        jobFramework!!.cancelAndJoin()
    }
})
