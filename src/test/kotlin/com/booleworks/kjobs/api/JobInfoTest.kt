// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.api

import com.booleworks.kjobs.api.persistence.hashmap.HashMapDataPersistence
import com.booleworks.kjobs.api.persistence.hashmap.HashMapJobPersistence
import com.booleworks.kjobs.common.TestInput
import com.booleworks.kjobs.common.TestResult
import com.booleworks.kjobs.common.defaultComputation
import com.booleworks.kjobs.common.defaultInstanceName
import com.booleworks.kjobs.common.defaultJobType
import com.booleworks.kjobs.common.jacksonObjectMapperWithTime
import com.booleworks.kjobs.common.ser
import com.booleworks.kjobs.data.Job
import com.booleworks.kjobs.data.JobStatus
import com.fasterxml.jackson.module.kotlin.readValue
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldBeOneOf
import io.kotest.matchers.equals.shouldBeEqual
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.application.call
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.route
import io.ktor.server.testing.testApplication
import kotlin.time.Duration.Companion.milliseconds

class JobInfoTest : FunSpec({

    test("test default config") {
        val jobPersistence = HashMapJobPersistence()
        val dataPersistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
        testApplication {
            routing {
                route("test") {
                    JobFramework(defaultInstanceName, jobPersistence) {
                        maintenanceConfig { jobCheckInterval = 500.milliseconds }
                        addApi(
                            defaultJobType,
                            this@route,
                            dataPersistence,
                            { call.receive<TestInput>() },
                            { call.respond<TestResult>(it) },
                            defaultComputation
                        ) {
                            infoConfig { enabled = true }
                        }
                    }
                }
            }
            val uuid = client.post("test/submit") { contentType(ContentType.Application.Json); setBody(TestInput().ser()) }.bodyAsText()
            val infoResponse = client.get("test/info/$uuid")
            infoResponse.status shouldBeEqual HttpStatusCode.OK
            jacksonObjectMapperWithTime().readValue<Job>(infoResponse.bodyAsText()).apply {
                this.uuid shouldBeEqual uuid
                type shouldBeEqual defaultJobType
                createdBy shouldBeEqual defaultInstanceName
                executingInstance shouldBeOneOf listOf(null, defaultInstanceName)
                status shouldBeOneOf listOf(JobStatus.CREATED, JobStatus.RUNNING, JobStatus.SUCCESS)
                numRestarts shouldBeEqual 0
            }
        }
    }

    test("test with custom text") {
        val jobPersistence = HashMapJobPersistence()
        val dataPersistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
        testApplication {
            routing {
                route("test") {
                    JobFramework(defaultInstanceName, jobPersistence) {
                        maintenanceConfig { jobCheckInterval = 500.milliseconds }
                        addApi(
                            defaultJobType,
                            this@route,
                            dataPersistence,
                            { call.receive<TestInput>() },
                            { call.respond<TestResult>(it) },
                            defaultComputation
                        ) {
                            infoConfig {
                                enabled = true
                                responder = { call.respond("Information about UUID ${it.uuid}") }
                            }
                        }
                    }
                }
            }
            val uuid = client.post("test/submit") { contentType(ContentType.Application.Json); setBody(TestInput().ser()) }.bodyAsText()
            val infoResponse = client.get("test/info/$uuid")
            infoResponse.status shouldBeEqual HttpStatusCode.OK
            infoResponse.bodyAsText() shouldBeEqual "Information about UUID $uuid"
        }
    }
})
