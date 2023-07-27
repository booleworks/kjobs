// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.api

import com.booleworks.kjobs.api.persistence.hashmap.HashMapDataPersistence
import com.booleworks.kjobs.api.persistence.hashmap.HashMapJobPersistence
import com.booleworks.kjobs.api.persistence.newJob
import com.booleworks.kjobs.common.TestInput
import com.booleworks.kjobs.common.TestResult
import com.booleworks.kjobs.common.defaultComputation
import com.booleworks.kjobs.common.defaultInstanceName
import com.booleworks.kjobs.data.JobStatistics
import com.booleworks.kjobs.data.JobStatus
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.equals.shouldBeEqual
import io.ktor.client.call.body
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.accept
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.call
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import io.ktor.server.routing.route
import io.ktor.server.testing.testApplication
import java.time.LocalDateTime
import kotlin.time.Duration.Companion.milliseconds

class StatisticsTest : FunSpec({

    val d1 = LocalDateTime.of(2023, 7, 22, 12, 0, 0)

    test("test overall statistics") {
        val jobPersistence = HashMapJobPersistence()
        val dataPersistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
        testApplication {
            val client = createClient { install(ContentNegotiation) { jackson { } } }
            routing {
                route("base") {
                    JobFramework(defaultInstanceName, jobPersistence) {
                        enableStatistics(this@route) {}
                        setupTwoJobApis(this@route, dataPersistence)
                    }
                }
            }
            client.get("base/statistics") { accept(ContentType.Application.Json) }.body<JobStatistics>() shouldBeEqual
                    JobStatistics(0, 0, 0, 0, 0, 0, 0, Double.NaN, Double.NaN)
            persistJobs(jobPersistence, d1)
            client.get("base/statistics") { accept(ContentType.Application.Json) }.body<JobStatistics>() shouldBeEqual
                    JobStatistics(12, 2, 2, 2, 2, 2, 2, 7.5, 10.0)
        }
    }

    test("test statistics per type") {
        val jobPersistence = HashMapJobPersistence()
        val dataPersistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
        testApplication {
            val client = createClient { install(ContentNegotiation) { jackson { } } }
            routing {
                route("base") {
                    JobFramework(defaultInstanceName, jobPersistence) {
                        enableStatistics(this@route) {
                            statisticsGenerator = { JobStatistics.byType(jobPersistence) }
                        }
                        setupTwoJobApis(this@route, dataPersistence)
                    }
                }
            }
            client.get("base/statistics") { accept(ContentType.Application.Json) }.body<Map<String, JobStatistics>>() shouldBeEqual emptyMap()
            persistJobs(jobPersistence, d1)
            client.get("base/statistics") { accept(ContentType.Application.Json) }.body<Map<String, JobStatistics>>() shouldBeEqual
                    mapOf(
                        "test1" to JobStatistics(6, 1, 1, 1, 1, 1, 1, 20.0 / 3, 5.0),
                        "test2" to JobStatistics(6, 1, 1, 1, 1, 1, 1, 25.0 / 3, 15.0),
                    )
        }
    }

    test("test override route") {
        val jobPersistence = HashMapJobPersistence()
        testApplication {
            val client = createClient { install(ContentNegotiation) { jackson { } } }
            routing {
                route("base") {
                    JobFramework(defaultInstanceName, jobPersistence) {
                        enableStatistics(this@route) {
                            routeDefinition = { get("hello") { call.respondText { "World" } } }
                        }
                    }
                }
            }
            client.get("base/hello").bodyAsText() shouldBeEqual "World"
        }
    }

    test("test override route and statistics per type") {
        val jobPersistence = HashMapJobPersistence()
        val dataPersistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
        testApplication {
            val client = createClient { install(ContentNegotiation) { jackson { } } }
            routing {
                route("base") {
                    JobFramework(defaultInstanceName, jobPersistence) {
                        enableStatistics(this@route) {
                            statisticsGenerator = { JobStatistics.byType(jobPersistence) }
                            routeDefinition = { get("hello") { call.respond(statisticsGenerator()) } }
                        }
                        setupTwoJobApis(this@route, dataPersistence)
                    }
                }
            }
            client.get("base/hello") { accept(ContentType.Application.Json) }.body<Map<String, JobStatistics>>() shouldBeEqual emptyMap()
            persistJobs(jobPersistence, d1)
            client.get("base/hello") { accept(ContentType.Application.Json) }.body<Map<String, JobStatistics>>() shouldBeEqual
                    mapOf(
                        "test1" to JobStatistics(6, 1, 1, 1, 1, 1, 1, 20.0 / 3, 5.0),
                        "test2" to JobStatistics(6, 1, 1, 1, 1, 1, 1, 25.0 / 3, 15.0),
                    )
        }
    }
})

private fun JobFrameworkBuilder.setupTwoJobApis(
    route: Route,
    dataPersistence: HashMapDataPersistence<TestInput, TestResult>
) {
    route.route("test1") {
        maintenanceConfig { jobCheckInterval = 500.milliseconds }
        addApi(
            "test1",
            this@route,
            dataPersistence,
            { call.receive<TestInput>() },
            { call.respond<TestResult>(it) },
            defaultComputation
        )
    }
    route.route("test2") {
        addApi(
            "test2",
            this@route,
            dataPersistence,
            { call.receive<TestInput>() },
            { call.respond<TestResult>(it) },
            defaultComputation
        )
    }
}

private suspend fun persistJobs(jobPersistence: HashMapJobPersistence, d1: LocalDateTime) {
    jobPersistence.persistJob(newJob("11", "test1", JobStatus.CREATED))
    jobPersistence.persistJob(newJob("12", "test1", JobStatus.RUNNING, createdAt = d1, startedAt = d1.plusSeconds(5)))
    jobPersistence.persistJob(newJob("13", "test1", JobStatus.SUCCESS, createdAt = d1, startedAt = d1.plusSeconds(5), finishedAt = d1.plusSeconds(10)))
    jobPersistence.persistJob(newJob("14", "test1", JobStatus.FAILURE, createdAt = d1, startedAt = d1.plusSeconds(10)))
    jobPersistence.persistJob(newJob("15", "test1", JobStatus.CANCEL_REQUESTED))
    jobPersistence.persistJob(newJob("16", "test1", JobStatus.CANCELLED))
    jobPersistence.persistJob(newJob("21", "test2", JobStatus.CREATED))
    jobPersistence.persistJob(newJob("22", "test2", JobStatus.RUNNING, createdAt = d1, startedAt = d1.plusSeconds(10)))
    jobPersistence.persistJob(newJob("23", "test2", JobStatus.SUCCESS, createdAt = d1, startedAt = d1.plusSeconds(0), finishedAt = d1.plusSeconds(15)))
    jobPersistence.persistJob(newJob("24", "test2", JobStatus.FAILURE, createdAt = d1, startedAt = d1.plusSeconds(15)))
    jobPersistence.persistJob(newJob("25", "test2", JobStatus.CANCEL_REQUESTED))
    jobPersistence.persistJob(newJob("26", "test2", JobStatus.CANCELLED))
}
