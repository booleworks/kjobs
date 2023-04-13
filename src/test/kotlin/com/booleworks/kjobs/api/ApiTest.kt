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
import io.ktor.server.routing.application
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
        val persistence = newRedisPersistence(defaultRedis)
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
        assertThat(submit.status).isEqualTo(HttpStatusCode.OK)
        val uuid = submit.bodyAsText().also { assertThat(UUID.fromString(it)).isNotNull() }
        assertThat(client.get("test/status/$uuid").bodyAsText()).isEqualTo("CREATED")
        delay(1.seconds)
        assertThat(client.get("test/status/$uuid").bodyAsText()).isEqualTo("SUCCESS")
        assertThat(jacksonObjectMapper().readValue<TestResult>(client.get("test/result/$uuid").bodyAsText())).isEqualTo(TestResult())
    }
}
