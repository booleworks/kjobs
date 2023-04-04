// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.jobframework.testservice

import com.booleworks.jobframework.boundary.impl.RedisPersistence
import com.booleworks.jobframework.data.Job
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
        throw TestException("Test Message")
    } else {
        TestResult(input.value)
    }
}

class TestException(message: String) : Exception(message)

fun Any.ser() = jacksonObjectMapperWithTime().writeValueAsBytes(this)
