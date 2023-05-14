// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.api

import com.booleworks.kjobs.common.Either
import com.booleworks.kjobs.common.TestInput
import com.booleworks.kjobs.common.TestResult
import com.booleworks.kjobs.common.defaultComputation
import com.booleworks.kjobs.common.defaultInstanceName
import com.booleworks.kjobs.common.defaultJobType
import com.booleworks.kjobs.common.defaultRedis
import com.booleworks.kjobs.common.newRedisPersistence
import com.booleworks.kjobs.common.ser
import com.booleworks.kjobs.common.testJobFramework
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
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
import io.ktor.server.routing.application
import io.ktor.server.routing.route
import kotlinx.coroutines.delay
import java.util.*
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class ApiTest : FunSpec({

    test("Test API") {
        testJobFramework {
            val persistence = newRedisPersistence<TestInput, TestResult>(defaultRedis)
            routing {
                route("test") {
                    JobFramework(defaultInstanceName, persistence, Either.Right(application)) {
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
            jacksonObjectMapper().readValue<TestResult>(client.get("test/result/$uuid").bodyAsText()) shouldBeEqual TestResult()
        }
    }

    test("Test API calls with wrong job type") {
        testJobFramework {
            val persistence = newRedisPersistence<TestInput, TestResult>(defaultRedis)
            routing {
                JobFramework(defaultInstanceName, persistence, Either.Right(application)) {
                    maintenanceConfig { jobCheckInterval = 50.milliseconds }
                    cancellationConfig { enabled = true }
                    route("test") {
                        addApi("testType", this@route, persistence, { call.receive<TestInput>() }, { call.respond<TestResult>(it) }, defaultComputation) {
                            apiConfig { enableDeletion = true }
                        }
                    }
                    route("test2") {
                        addApi("testType2", this@route, persistence, { call.receive<TestInput>() }, { call.respond<TestResult>(it) }, defaultComputation) {
                            apiConfig { enableDeletion = true }
                        }
                    }
                }
            }
            val uuid1 = client.post("test/submit") { contentType(ContentType.Application.Json); setBody(TestInput().ser()) }.bodyAsText()
            val uuid2 = client.post("test2/submit") { contentType(ContentType.Application.Json); setBody(TestInput().ser()) }.bodyAsText()
            delay(500.milliseconds)
            
            client.get("test2/status/$uuid1").shouldBeWrongJobTypeBadRequest()
            client.get("test/status/$uuid2").shouldBeWrongJobTypeBadRequest()
            client.get("test2/result/$uuid1").shouldBeWrongJobTypeBadRequest()
            client.get("test/result/$uuid2").shouldBeWrongJobTypeBadRequest()
            client.post("test2/cancel/$uuid1").shouldBeWrongJobTypeBadRequest()
            client.post("test/cancel/$uuid2").shouldBeWrongJobTypeBadRequest()
            client.delete("test2/delete/$uuid1").shouldBeWrongJobTypeBadRequest()
            client.delete("test/delete/$uuid2").shouldBeWrongJobTypeBadRequest()

            client.get("test/status/$uuid1").bodyAsText() shouldBeEqual "SUCCESS"
            client.get("test2/status/$uuid2").bodyAsText() shouldBeEqual "SUCCESS"
            jacksonObjectMapper().readValue<TestResult>(client.get("test/result/$uuid1").bodyAsText()) shouldBeEqual TestResult()
            jacksonObjectMapper().readValue<TestResult>(client.get("test2/result/$uuid2").bodyAsText()) shouldBeEqual TestResult()
            client.post("test/cancel/$uuid1").status shouldBeEqual HttpStatusCode.OK
            client.post("test2/cancel/$uuid2").status shouldBeEqual HttpStatusCode.OK
            client.delete("test/delete/$uuid1").status shouldBeEqual HttpStatusCode.OK
            client.delete("test2/delete/$uuid2").status shouldBeEqual HttpStatusCode.OK
        }
    }
})

suspend fun HttpResponse.shouldBeWrongJobTypeBadRequest() {
    this.status shouldBeEqual HttpStatusCode.BadRequest
    this.bodyAsText() shouldBeEqual "Illegal job type. A job with the given uuid was found, but it was created from a different resource."
}
