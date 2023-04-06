// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

@file:OptIn(ExperimentalCoroutinesApi::class)

package com.booleworks.jobframework.util

import com.booleworks.jobframework.boundary.Persistence
import com.booleworks.jobframework.boundary.impl.RedisPersistence
import com.booleworks.jobframework.control.JobExecutor
import com.booleworks.jobframework.data.DefaultExecutionCapacityProvider
import com.booleworks.jobframework.data.DefaultJobPrioritizer
import com.booleworks.jobframework.data.ExecutionCapacityProvider
import com.booleworks.jobframework.data.Job
import com.booleworks.jobframework.data.JobPrioritizer
import com.booleworks.jobframework.data.TagMatcher
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.github.fppt.jedismock.RedisServer
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import redis.clients.jedis.JedisPool
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

fun jacksonObjectMapperWithTime(): ObjectMapper = jacksonObjectMapper().registerModule(JavaTimeModule())

data class TestInput(val value: Int = 0, val expectedDelay: Int = 1, val throwException: Boolean = false)

data class TestResult(val inputValue: Int = 0)

fun newRedisPersistence(redis: RedisServer) = RedisPersistence<TestInput, TestResult>(
    JedisPool(redis.host, redis.bindPort),
    { jacksonObjectMapperWithTime().writeValueAsBytes(it) },
    { jacksonObjectMapperWithTime().writeValueAsBytes(it) },
    { jacksonObjectMapperWithTime().readValue(it) },
    { jacksonObjectMapperWithTime().readValue(it) })

val defaultComputation: suspend (Job, TestInput) -> TestResult = { _, input ->
    delay(input.expectedDelay.milliseconds)
    if (input.throwException) {
        throw TestException("Test Exception Message")
    } else {
        TestResult(input.value)
    }
}

const val defaultInstanceName: String = "ME"

internal fun defaultExecutor(
    persistence: Persistence<TestInput, TestResult>,
    myInstanceName: String = defaultInstanceName,
    executionCapacityProvider: ExecutionCapacityProvider = DefaultExecutionCapacityProvider,
    timeout: (Job, TestInput) -> Duration = { _, _ -> 100.seconds },
    jobPrioritizer: JobPrioritizer = DefaultJobPrioritizer,
    tagMatcher: TagMatcher = TagMatcher.Any,
) = JobExecutor(
    persistence,
    myInstanceName,
    defaultComputation,
    executionCapacityProvider,
    timeout,
    jobPrioritizer,
    tagMatcher
)

class TestException(message: String) : Exception(message)

fun Any.ser() = jacksonObjectMapperWithTime().writeValueAsBytes(this)

fun testWithRedis(block: suspend RedisPersistence<TestInput, TestResult>.() -> Unit) = runBlocking {
    val redis = RedisServer.newRedisServer().start()
    with(newRedisPersistence(redis)) { block() }
    redis.stop()
}

fun <R> Either<*, R>.right() = (this as Either.Right<R>).value
