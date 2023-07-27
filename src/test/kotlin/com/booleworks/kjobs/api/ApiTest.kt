// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.api

import com.booleworks.kjobs.common.TestInput
import com.booleworks.kjobs.common.TestResult
import com.booleworks.kjobs.common.defaultComputation
import com.booleworks.kjobs.common.defaultInstanceName
import com.booleworks.kjobs.common.defaultJobType
import com.booleworks.kjobs.common.defaultRedis
import com.booleworks.kjobs.common.newRedisPersistence
import com.booleworks.kjobs.common.parseTestResult
import com.booleworks.kjobs.common.ser
import com.booleworks.kjobs.common.testJobFrameworkWithRedis
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.equals.shouldBeEqual
import io.kotest.matchers.nulls.shouldNotBeNull
import io.ktor.client.request.delete
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.application.call
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.route
import kotlinx.coroutines.delay
import java.util.*
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class ApiTest : FunSpec({

    testJobFrameworkWithRedis("test API") {
        val persistence = newRedisPersistence<TestInput, TestResult>(defaultRedis)
        routing {
            route("test") {
                JobFramework(defaultInstanceName, persistence) {
                    maintenanceConfig { jobCheckInterval = 500.milliseconds }
                    addApi(defaultJobType, this@route, persistence, { call.receive<TestInput>() }, { call.respond<TestResult>(it) }, defaultComputation)
                }
            }
        }
        delay(100.milliseconds) // first run of executor should have started
        val submit = client.post("test/submit") { contentType(ContentType.Application.Json); setBody(TestInput().ser()) }
        submit.status shouldBeEqual HttpStatusCode.OK
        val uuid = submit.bodyAsText().also { UUID.fromString(it).shouldNotBeNull() }
        client.get("test/status/$uuid").bodyAsText() shouldBeEqual "CREATED"
        delay(1.seconds)
        client.get("test/status/$uuid").bodyAsText() shouldBeEqual "SUCCESS"
        client.get("test/result/$uuid").parseTestResult() shouldBeEqual TestResult()
    }

    testJobFrameworkWithRedis("test API calls with wrong job type") {
        val persistence = newRedisPersistence<TestInput, TestResult>(defaultRedis)
        routing {
            JobFramework(defaultInstanceName, persistence) {
                maintenanceConfig { jobCheckInterval = 50.milliseconds }
                enableCancellation {}
                route("test") {
                    addApi("testType", this@route, persistence, { call.receive<TestInput>() }, { call.respond<TestResult>(it) }, defaultComputation) {
                        apiConfig { enableDeletion = true }
                        infoConfig { enabled = true }
                    }
                }
                route("test2") {
                    addApi("testType2", this@route, persistence, { call.receive<TestInput>() }, { call.respond<TestResult>(it) }, defaultComputation) {
                        apiConfig { enableDeletion = true }
                        infoConfig { enabled = true }
                    }
                }
            }
        }
        val uuid1 = client.post("test/submit") { contentType(ContentType.Application.Json); setBody(TestInput().ser()) }.bodyAsText()
        val uuid2 = client.post("test2/submit") { contentType(ContentType.Application.Json); setBody(TestInput().ser()) }.bodyAsText()
        delay(500.milliseconds)

        client.get("test2/status/$uuid1").shouldBeWrongJobTypeBadRequest()
        client.get("test/status/$uuid2").shouldBeWrongJobTypeBadRequest()
        client.get("test2/info/$uuid1").shouldBeWrongJobTypeBadRequest()
        client.get("test/info/$uuid2").shouldBeWrongJobTypeBadRequest()
        client.get("test2/result/$uuid1").shouldBeWrongJobTypeBadRequest()
        client.get("test/result/$uuid2").shouldBeWrongJobTypeBadRequest()
        client.post("test2/cancel/$uuid1").shouldBeWrongJobTypeBadRequest()
        client.post("test/cancel/$uuid2").shouldBeWrongJobTypeBadRequest()
        client.delete("test2/delete/$uuid1").shouldBeWrongJobTypeBadRequest()
        client.delete("test/delete/$uuid2").shouldBeWrongJobTypeBadRequest()

        client.get("test/status/$uuid1").bodyAsText() shouldBeEqual "SUCCESS"
        client.get("test2/status/$uuid2").bodyAsText() shouldBeEqual "SUCCESS"
        client.get("test/info/$uuid1").status shouldBeEqual HttpStatusCode.OK
        client.get("test2/info/$uuid2").status shouldBeEqual HttpStatusCode.OK
        client.get("test/result/$uuid1").parseTestResult() shouldBeEqual TestResult()
        client.get("test2/result/$uuid2").parseTestResult() shouldBeEqual TestResult()
        client.post("test/cancel/$uuid1").status shouldBeEqual HttpStatusCode.OK
        client.post("test2/cancel/$uuid2").status shouldBeEqual HttpStatusCode.OK
        client.delete("test/delete/$uuid1").status shouldBeEqual HttpStatusCode.OK
        client.delete("test2/delete/$uuid2").status shouldBeEqual HttpStatusCode.OK
    }

    testJobFrameworkWithRedis("test API calls with reconfigured resources") {
        val persistence = newRedisPersistence<TestInput, TestResult>(defaultRedis)
        routing {
            JobFramework(defaultInstanceName, persistence) {
                maintenanceConfig { jobCheckInterval = 20.milliseconds }
                enableCancellation { }
                route("test") {
                    addApi("testType", this@route, persistence, { call.receive<TestInput>() }, { call.respond<TestResult>(it) }, defaultComputation) {
                        synchronousResourceConfig { enabled = true }
                        infoConfig { enabled = true }
                        apiConfig {
                            enableDeletion = true
                            submitRoute = { block -> post("sub") { block() } }
                            statusRoute = { block -> get("stat/{uuid}") { block() } }
                            resultRoute = { block -> get("res/{uuid}") { block() } }
                            failureRoute = { block -> get("fail/{uuid}") { block() } }
                            deleteRoute = { block -> get("del/{uuid}") { block() } }
                            cancelRoute = { block -> post("cancellation/{uuid}") { block() } }
                            syncRoute = { block -> post("sync") { block() } }
                            infoRoute = { block -> post("info/{uuid}") { block() } }
                        }
                    }
                }
            }
        }

        val uuid = client.post("test/sub") { contentType(ContentType.Application.Json); setBody(TestInput().ser()) }
            .shouldHaveStatus(HttpStatusCode.OK).bodyAsText().shouldNotBeNull()
        delay(100.milliseconds)
        client.get("test/stat/$uuid").shouldHaveStatus(HttpStatusCode.OK)
        client.get("test/res/$uuid").shouldHaveStatus(HttpStatusCode.OK)
        client.post("test/cancellation/$uuid").shouldHaveStatus(HttpStatusCode.OK)
        client.post("test/info/$uuid").shouldHaveStatus(HttpStatusCode.OK)
        client.get("test/del/$uuid").shouldHaveStatus(HttpStatusCode.OK)

        val uuidFail = client.post("test/sub") { contentType(ContentType.Application.Json); setBody(TestInput(throwException = true).ser()) }
            .shouldHaveStatus(HttpStatusCode.OK).bodyAsText().shouldNotBeNull()
        client.post("test/sync") { contentType(ContentType.Application.Json); setBody(TestInput().ser()) }.shouldHaveStatus(HttpStatusCode.OK).bodyAsText()
            .shouldNotBeNull()
        client.get("test/fail/$uuidFail").shouldHaveStatus(HttpStatusCode.OK)

        client.post("test/submit") { contentType(ContentType.Application.Json); setBody(TestInput(throwException = true).ser()) }
            .shouldHaveStatus(HttpStatusCode.NotFound)
        client.get("test/status/$uuid").shouldHaveStatus(HttpStatusCode.NotFound)
        client.get("test/result/$uuid").shouldHaveStatus(HttpStatusCode.NotFound)
        client.get("test/failure/$uuid").shouldHaveStatus(HttpStatusCode.NotFound)
        client.post("test/cancel/$uuid").shouldHaveStatus(HttpStatusCode.NotFound)
        client.delete("test/delete/$uuid").shouldHaveStatus(HttpStatusCode.NotFound)
        client.get("test/info/$uuid").shouldHaveStatus(HttpStatusCode.NotFound)
        client.post("test/synchronous") { contentType(ContentType.Application.Json); setBody(TestInput(throwException = true).ser()) }
            .shouldHaveStatus(HttpStatusCode.NotFound)
    }

    testJobFrameworkWithRedis("test input validation") {
        val persistence = newRedisPersistence<TestInput, TestResult>(defaultRedis)
        routing {
            JobFramework(defaultInstanceName, persistence) {
                maintenanceConfig { jobCheckInterval = 20.milliseconds }
                route("test") {
                    addApi("testType", this@route, persistence, { call.receive<TestInput>() }, { call.respond<TestResult>(it) }, defaultComputation) {
                        synchronousResourceConfig { enabled = true }
                        apiConfig {
                            inputValidation = { if (it.value >= 0) emptyList() else listOf("Value must not be negative", "Some second message") }
                        }
                    }
                }
            }
        }
        client.post("test/submit") { contentType(ContentType.Application.Json); setBody(TestInput(-1).ser()) }
            .shouldHaveStatus(HttpStatusCode.BadRequest).bodyAsText() shouldBeEqual "Value must not be negative, Some second message"
        client.post("test/synchronous") { contentType(ContentType.Application.Json); setBody(TestInput(-1).ser()) }
            .shouldHaveStatus(HttpStatusCode.BadRequest).bodyAsText() shouldBeEqual "Value must not be negative, Some second message"
        val uuid = client.post("test/submit") { contentType(ContentType.Application.Json); setBody(TestInput(0).ser()) }
            .shouldHaveStatus(HttpStatusCode.OK).bodyAsText().shouldNotBeNull()
        client.post("test/synchronous") { contentType(ContentType.Application.Json); setBody(TestInput(0).ser()) }
            .shouldHaveStatus(HttpStatusCode.OK).bodyAsText() shouldBeEqual "{\n  \"inputValue\" : 0\n}"
        client.get("test/result/$uuid").shouldHaveStatus(HttpStatusCode.OK).bodyAsText() shouldBeEqual "{\n  \"inputValue\" : 0\n}"
    }
})

suspend fun HttpResponse.shouldBeWrongJobTypeBadRequest() {
    this.status shouldBeEqual HttpStatusCode.BadRequest
    this.bodyAsText() shouldBeEqual "Illegal job type. A job with the given uuid was found, but it was created from a different resource."
}

fun HttpResponse.shouldHaveStatus(code: HttpStatusCode) = this.also {
    this.status shouldBeEqual code
}
