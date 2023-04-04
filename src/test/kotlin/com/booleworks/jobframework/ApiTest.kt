// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.jobframework

import com.booleworks.jobframework.boundary.JobFramework
import com.booleworks.jobframework.testservice.TestInput
import com.booleworks.jobframework.testservice.TestResult
import com.booleworks.jobframework.testservice.defaultComputation
import com.booleworks.jobframework.testservice.defaultRedis
import com.booleworks.jobframework.testservice.newRedisPersistence
import com.booleworks.jobframework.testservice.ser
import com.booleworks.jobframework.testservice.testJobFramework
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
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
import kotlinx.coroutines.delay
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class ApiTest {

    @Test
    fun test() = testJobFramework {
        routing {
            route("test") {
                JobFramework.newApi(
                    this@route,
                    newRedisPersistence(defaultRedis),
                    "ME",
                    { call.receive<TestInput>() },
                    { call.respond<TestResult>(it) },
                    defaultComputation
                ) {
                    maintenanceConfig { jobCheckInterval = 500.milliseconds }
                }
            }
        }
        delay(100.milliseconds) // first run of executor should have started
        val submit = client.post("test/submit") { contentType(ContentType.Application.Json); setBody(TestInput().ser()) }
        assertThat(submit.status).isEqualTo(HttpStatusCode.OK)
        val uuid = submit.bodyAsText().also { assertThat(UUID.fromString(it)).isNotNull() }
        assertThat(client.get("test/status/$uuid").bodyAsText()).isEqualTo("CREATED")
        delay(1.seconds)
        assertThat(client.get("test/status/$uuid").bodyAsText()).isEqualTo("SUCCESS")
        assertThat(jacksonObjectMapper().readValue<TestResult>(client.get("test/result/$uuid").bodyAsText())).isEqualTo(TestResult())
    }
}
