// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.api

import com.booleworks.kjobs.api.hierarchical.HierarchicalJobApi
import com.booleworks.kjobs.api.impl.RedisDataPersistence
import com.booleworks.kjobs.common.Either
import com.booleworks.kjobs.common.TestInput
import com.booleworks.kjobs.common.TestResult
import com.booleworks.kjobs.common.defaultInstanceName
import com.booleworks.kjobs.common.defaultJobType
import com.booleworks.kjobs.common.defaultRedis
import com.booleworks.kjobs.common.newRedisPersistence
import com.booleworks.kjobs.common.ser
import com.booleworks.kjobs.common.testJobFramework
import com.booleworks.kjobs.control.ComputationResult
import com.booleworks.kjobs.data.ExecutionCapacity.Companion.AcceptingAnyJob
import com.booleworks.kjobs.data.ExecutionCapacityProvider
import com.booleworks.kjobs.data.Job
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
import io.ktor.server.testing.ApplicationTestBuilder
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.filterIsInstance
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class HierarchicalApiTest {

    private val subJob1 = "SUB_JOB_1"
    private val subJob2 = "SUB_JOB_2"

    @Test
    fun testMultipleJobs() = testJobFramework {
        val persistence = newRedisPersistence<TestInput, TestResult>(defaultRedis)
        addTestRoute(persistence, { _, input ->
            delay(input.a.milliseconds); ComputationResult.Success(SubTestResult1(input.a))
        }) { _, input ->
            delay(input.b.milliseconds); ComputationResult.Success(SubTestResult2(input.b - input.a))
        }
        val submit = client.post("test/submit") { contentType(ContentType.Application.Json); setBody(TestInput(5000).ser()) }
        assertThat(submit.status).isEqualTo(HttpStatusCode.OK)
        val uuid = submit.bodyAsText().also { assertThat(UUID.fromString(it)).isNotNull() }
        delay(300.milliseconds)
        assertThat(client.get("test/status/$uuid").bodyAsText()).isEqualTo("RUNNING")
        delay(3.seconds)
        println(client.get("test/failure/$uuid").bodyAsText())
        assertThat(client.get("test/status/$uuid").bodyAsText()).isEqualTo("SUCCESS")
        assertThat(jacksonObjectMapper().readValue<TestResult>(client.get("test/result/$uuid").bodyAsText())).isEqualTo(TestResult(1100))

        val submit2 = client.post("test/submit") { contentType(ContentType.Application.Json); setBody(TestInput(500).ser()) }
        assertThat(submit2.status).isEqualTo(HttpStatusCode.OK)
        val uuid2 = submit2.bodyAsText().also { assertThat(UUID.fromString(it)).isNotNull() }
        delay(300.milliseconds)
        assertThat(client.get("test/status/$uuid2").bodyAsText()).isEqualTo("RUNNING")
        delay(3.seconds)
        println(client.get("test/failure/$uuid2").bodyAsText())
        assertThat(client.get("test/status/$uuid2").bodyAsText()).isEqualTo("SUCCESS")
        assertThat(jacksonObjectMapper().readValue<TestResult>(client.get("test/result/$uuid2").bodyAsText())).isEqualTo(TestResult(-500))
    }

    private fun ApplicationTestBuilder.addTestRoute(
        persistence: RedisDataPersistence<TestInput, TestResult>,
        computation1: suspend (Job, SubTestInput1) -> ComputationResult<SubTestResult1>,
        computation2: suspend (Job, SubTestInput2) -> ComputationResult<SubTestResult2>
    ) {
        routing {
            route("test") {
                JobFramework(defaultInstanceName, persistence, Either.Right(application)) {
                    maintenanceConfig { jobCheckInterval = 20.milliseconds }
                    executorConfig { executionCapacityProvider = ExecutionCapacityProvider { AcceptingAnyJob } }
                    addApiForHierarchicalJob(
                        defaultJobType, this@route, persistence,
                        { call.receive<TestInput>() }, { call.respond<TestResult>(it) }, superComputation()
                    ) {
                        addDependentJob(subJob1, newRedisPersistence<SubTestInput1, SubTestResult1>(), computation1) {}
                        addDependentJob(subJob2, newRedisPersistence<SubTestInput2, SubTestResult2>(), computation2) {}
                    }
                }
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun superComputation() = { _: Job, input: TestInput, apis: Map<String, HierarchicalJobApi<*, *>> ->
        val api1 = apis[subJob1] as HierarchicalJobApi<SubTestInput1, SubTestResult1>
        val api2 = apis[subJob2] as HierarchicalJobApi<SubTestInput2, SubTestResult2>
        launchJobs(input, api1, api2)
    }

    private fun launchJobs(
        input: TestInput,
        api1: HierarchicalJobApi<SubTestInput1, SubTestResult1>,
        api2: HierarchicalJobApi<SubTestInput2, SubTestResult2>,
    ): ComputationResult<TestResult> = runBlocking {
        val resultFlow = merge(
            api1.waitForDependents().map { it.second }
                .filterIsInstance<ComputationResult.Success<SubTestResult1>>()
                .filter { it.result.a > 1000 }
                .map { it.toTestResult() },
            api2.waitForDependents().map { it.second.toTestResult() }
        ).catch { ComputationResult.Error(it.message ?: "") }
        for (i in 0.rangeTo(input.value).step(100)) {
            api1.submitDependentJob(SubTestInput1(i, input.value))
            api2.submitDependentJob(SubTestInput2(i, input.value))
        }
        resultFlow.first()
    }
}

data class SubTestInput1(val a: Int, val b: Int)
data class SubTestResult1(val a: Int)
data class SubTestInput2(val a: Int, val b: Int)
data class SubTestResult2(val a: Int)

@JvmName("toTestResult1")
fun ComputationResult<SubTestResult1>.toTestResult(): ComputationResult<TestResult> = when (this) {
    is ComputationResult.Success -> ComputationResult.Success(TestResult(this.result.a))
    is ComputationResult.Error -> this
    else -> TODO("Why is this necessary?")
}

@JvmName("toTestResult2")
fun ComputationResult<SubTestResult2>.toTestResult(): ComputationResult<TestResult> = when (this) {
    is ComputationResult.Success -> ComputationResult.Success(TestResult(-this.result.a))
    is ComputationResult.Error -> this
    else -> TODO("Why is this necessary?")
}
