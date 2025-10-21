// SPDX-License-Identifier: MIT
// Copyright 2023-2024 BooleWorks GmbH

package com.booleworks.kjobs.api.hierarchical

import com.booleworks.kjobs.api.JobFramework
import com.booleworks.kjobs.common.defaultInstanceName
import com.booleworks.kjobs.common.newRedisPersistence
import com.booleworks.kjobs.common.parse
import com.booleworks.kjobs.common.ser
import com.booleworks.kjobs.common.testJobFrameworkWithRedis
import com.booleworks.kjobs.control.ComputationResult
import com.booleworks.kjobs.data.ExecutionCapacity.Companion.AcceptingAnyJob
import com.booleworks.kjobs.data.ExecutionCapacityProvider
import com.booleworks.kjobs.data.Job
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.equals.shouldBeEqual
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.contentType
import io.ktor.server.application.call
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.route
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.toList
import kotlin.time.Duration.Companion.milliseconds

class HierarchicalApiTest2 : FunSpec({
    testJobFrameworkWithRedis("hierarchical api test 2") {
        val intPersistence = newRedisPersistence<Int, Int>()
        val stringPersistence = newRedisPersistence<String, String>()
        val listPersistence = newRedisPersistence<TestInputList, TestResultList>()

        var jobFramework: kotlinx.coroutines.Job? = null
        routing {
            route("split-job") {
                jobFramework = JobFramework(defaultInstanceName, intPersistence) {
                    addApiForHierarchicalJob(
                        "splitJob", this@route, listPersistence, { receive<TestInputList>() }, { respond(it) }, parentComputation
                    ) {
                        addDependentJob("int-computation", intPersistence, { _, input -> ComputationResult.Success(input + 1) }) {}
                        addDependentJob("string-computation", stringPersistence, { _, input -> ComputationResult.Success("$input!") }) {}
                    }
                    maintenanceConfig { jobCheckInterval = 20.milliseconds }
                    executorConfig { executionCapacityProvider = ExecutionCapacityProvider { AcceptingAnyJob } }
                }
            }
        }

        val uuid = client.post("split-job/submit") {
            contentType(ContentType.Application.Json)
            setBody(TestInputList(listOf(42, 1, 5, 9), listOf("Hello", "World")).ser())
        }.bodyAsText()
        delay(30.milliseconds)
        client.get("split-job/status/$uuid").bodyAsText() shouldBeEqual "RUNNING"
        delay(500.milliseconds)
        client.get("split-job/status/$uuid").bodyAsText() shouldBeEqual "SUCCESS"
        client.get("split-job/result/$uuid").parse<TestResultList>() shouldBeEqual TestResultList(listOf(43, 2, 6, 10), listOf("Hello!", "World!"))
        jobFramework!!.cancelAndJoin()
    }
})

private data class TestInputList(val values1: List<Int>, val values2: List<String>)
private data class TestResultList(val values1: List<Int>, val values2: List<String>)

@Suppress("UNCHECKED_CAST")
private val parentComputation: suspend (Job, TestInputList, Map<String, HierarchicalJobApi<*, *>>) -> ComputationResult<TestResultList> =
    { _, inputList, apis ->
        val intApi = apis["int-computation"] as HierarchicalJobApi<Int, Int>
        val stringApi = apis["string-computation"] as HierarchicalJobApi<String, String>

        val intJobs = inputList.values1.map { intApi.submitDependentJob(it)!!.uuid }
        val stringJobs = inputList.values2.map { stringApi.submitDependentJob(it)!!.uuid }

        val intResults = intApi.collectDependentResults(checkInterval = 10.milliseconds).toList().associate { it }
            .mapValues { (it.value as ComputationResult.Success).result }
        val stringResults = stringApi.collectDependentResults(checkInterval = 10.milliseconds).toList().associate { it }
            .mapValues { (it.value as ComputationResult.Success).result }
        ComputationResult.Success(TestResultList(intJobs.map { intResults[it]!! }, stringJobs.map { stringResults[it]!! }))
    }
