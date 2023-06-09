// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.common

import com.booleworks.kjobs.api.DEFAULT_MAX_JOB_RESTARTS
import com.booleworks.kjobs.api.JobFrameworkBuilder
import com.booleworks.kjobs.api.persistence.DataPersistence
import com.booleworks.kjobs.api.persistence.hashmap.HashMapJobPersistence
import com.booleworks.kjobs.api.persistence.redis.RedisDataPersistence
import com.booleworks.kjobs.control.ComputationResult
import com.booleworks.kjobs.control.MainJobExecutor
import com.booleworks.kjobs.control.SpecificExecutor
import com.booleworks.kjobs.data.DefaultExecutionCapacityProvider
import com.booleworks.kjobs.data.DefaultJobPrioritizer
import com.booleworks.kjobs.data.ExecutionCapacityProvider
import com.booleworks.kjobs.data.Job
import com.booleworks.kjobs.data.JobPrioritizer
import com.booleworks.kjobs.data.JobStatus
import com.booleworks.kjobs.data.PersistenceAccessResult
import com.booleworks.kjobs.data.TagMatcher
import com.booleworks.kjobs.data.orQuitWith
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.github.fppt.jedismock.RedisServer
import io.kotest.assertions.fail
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldBeOneOf
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import redis.clients.jedis.JedisPool
import java.time.LocalDateTime
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

fun jacksonObjectMapperWithTime(): ObjectMapper = jacksonObjectMapper().registerModule(JavaTimeModule())

data class TestInput(val value: Int = 0, val expectedDelay: Int = 1, val throwException: Boolean = false)

data class TestResult(val inputValue: Int = 0)

inline fun <reified INPUT, reified RESULT> newRedisPersistence(redis: RedisServer = defaultRedis) = RedisDataPersistence<INPUT, RESULT>(
    JedisPool(redis.host, redis.bindPort),
    { jacksonObjectMapperWithTime().writeValueAsBytes(it) },
    { jacksonObjectMapperWithTime().writeValueAsBytes(it) },
    { jacksonObjectMapperWithTime().readValue(it) },
    { jacksonObjectMapperWithTime().readValue(it) })

val defaultComputation: suspend (Job, TestInput) -> ComputationResult<TestResult> = { _, input ->
    delay(input.expectedDelay.milliseconds)
    if (input.throwException) {
        throw TestException("Test Exception Message")
    } else {
        ComputationResult.Success(TestResult(input.value))
    }
}

const val defaultJobType: String = "TestJob"
const val defaultInstanceName: String = "ME"

internal fun defaultExecutor(
    persistence: DataPersistence<TestInput, TestResult>,
    myInstanceName: String = defaultInstanceName,
    executionCapacityProvider: ExecutionCapacityProvider = DefaultExecutionCapacityProvider,
    timeout: (Job, TestInput) -> Duration = { _, _ -> 100.seconds },
    maxRestarts: Int = DEFAULT_MAX_JOB_RESTARTS,
    jobPrioritizer: JobPrioritizer = DefaultJobPrioritizer,
    tagMatcher: TagMatcher = TagMatcher.Any,
    cancellationConfig: JobFrameworkBuilder.CancellationConfig = JobFrameworkBuilder.CancellationConfig(),
    specificExecutors: Map<String, SpecificExecutor<*, *>>? = null
) = MainJobExecutor(
    persistence,
    myInstanceName,
    executionCapacityProvider,
    jobPrioritizer,
    tagMatcher,
    cancellationConfig,
    specificExecutors ?: mapOf(defaultJobType to SpecificExecutor(myInstanceName, persistence, defaultComputation, timeout, maxRestarts)),
)

class TestException(message: String) : Exception(message)

fun Any.ser() = jacksonObjectMapperWithTime().writeValueAsBytes(this)

suspend fun HttpResponse.parseTestResult(): TestResult = jacksonObjectMapper().readValue<TestResult>(this.bodyAsText())
suspend inline fun <reified T> HttpResponse.parse(): T = jacksonObjectMapper().readValue<T>(this.bodyAsText())

fun FunSpec.testWithRedis(name: String, block: suspend RedisDataPersistence<TestInput, TestResult>.() -> Unit) = test(name) {
    runBlocking {
        val redis = RedisServer.newRedisServer().start()
        with(newRedisPersistence<TestInput, TestResult>(redis)) { block() }
        redis.stop()
    }
}

fun <R> Either<*, R>.right() = (this as Either.Right<R>).value

fun <R> PersistenceAccessResult<R>.expectSuccess() = this.orQuitWith { fail("Expected Persistence Access to succeed") }

fun Job.shouldHaveBeenStarted() = this.status shouldBeOneOf listOf(JobStatus.RUNNING, JobStatus.SUCCESS)

suspend fun Job.setRunning(jobPersistence: HashMapJobPersistence, instanceName: String = defaultInstanceName) {
    status = JobStatus.RUNNING
    executingInstance = instanceName
    startedAt = LocalDateTime.now()
    jobPersistence.updateJob(this).expectSuccess()
}

suspend fun Job.reset(jobPersistence: HashMapJobPersistence) {
    status = JobStatus.CREATED
    executingInstance = null
    startedAt = null
    finishedAt = null
    timeout = null
    jobPersistence.updateJob(this).expectSuccess()
}
