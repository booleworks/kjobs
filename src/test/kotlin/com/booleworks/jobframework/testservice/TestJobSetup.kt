// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.jobframework.testservice

import com.booleworks.jobframework.boundary.impl.RedisPersistence
import com.booleworks.jobframework.data.Job
import com.booleworks.jobframework.data.JobInput
import com.booleworks.jobframework.data.JobResult
import com.fasterxml.jackson.annotation.JsonAutoDetect
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.github.fppt.jedismock.RedisServer
import kotlinx.coroutines.delay
import redis.clients.jedis.JedisPool
import kotlin.time.Duration.Companion.milliseconds

fun jacksonObjectMapperWithTime(): ObjectMapper = jacksonObjectMapper().registerModule(JavaTimeModule())

data class TestInput(val value: Int = 0, val expectedDelay: Int = 1, val throwException: Boolean = false)

@JsonAutoDetect(
    getterVisibility = JsonAutoDetect.Visibility.NONE,
    isGetterVisibility = JsonAutoDetect.Visibility.NONE,
    setterVisibility = JsonAutoDetect.Visibility.NONE,
    creatorVisibility = JsonAutoDetect.Visibility.NONE,
    fieldVisibility = JsonAutoDetect.Visibility.ANY
)
class TestJobInput(val input: TestInput) : JobInput<TestInput> {
    override fun data() = input
    override fun serializedData() = jacksonObjectMapperWithTime().writeValueAsBytes(input)
}

data class TestResult(val inputValue: Int = 0)

@JsonAutoDetect(
    getterVisibility = JsonAutoDetect.Visibility.NONE,
    isGetterVisibility = JsonAutoDetect.Visibility.NONE,
    setterVisibility = JsonAutoDetect.Visibility.NONE,
    creatorVisibility = JsonAutoDetect.Visibility.NONE,
    fieldVisibility = JsonAutoDetect.Visibility.ANY
)
class TestJobResult(override val uuid: String, val result: TestResult?, val error: String?) : JobResult<TestResult> {
    override fun isSuccess() = result != null
    override fun result() = result
    override fun serializedResult() = jacksonObjectMapperWithTime().writeValueAsBytes(result)
    override fun error() = error
}

fun newRedisPersistence(redis: RedisServer) = RedisPersistence<TestInput, TestResult, TestJobInput, TestJobResult>(
    JedisPool(redis.host, redis.bindPort),
    { it.serializedData() },
    { jacksonObjectMapperWithTime().writeValueAsBytes(it) },
    { TestJobInput(jacksonObjectMapperWithTime().readValue(it)) },
    { jacksonObjectMapperWithTime().readValue(it) })

val defaultComputation: suspend (Job, TestJobInput) -> TestJobResult = { job, input ->
    delay(input.input.expectedDelay.milliseconds)
    if (input.input.throwException) {
        throw TestException("Test Message")
    } else {
        TestJobResult(job.uuid, TestResult(input.input.value), null)
    }
}

class TestException(message: String) : Exception(message)

fun Any.ser() = jacksonObjectMapperWithTime().writeValueAsBytes(this)
