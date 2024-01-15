// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.api

import com.booleworks.kjobs.api.persistence.hashmap.HashMapDataPersistence
import com.booleworks.kjobs.api.persistence.hashmap.HashMapJobPersistence
import com.booleworks.kjobs.common.TestInput
import com.booleworks.kjobs.common.TestResult
import com.booleworks.kjobs.common.defaultComputation
import com.booleworks.kjobs.common.defaultInstanceName
import com.booleworks.kjobs.common.lettuceClient
import com.booleworks.kjobs.control.polling.RedisLongPollManager
import com.booleworks.kjobs.data.ExecutionCapacity
import com.booleworks.kjobs.data.ExecutionCapacityProvider
import com.github.fppt.jedismock.RedisServer
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.comparables.shouldBeGreaterThan
import io.kotest.matchers.comparables.shouldBeLessThan
import io.kotest.matchers.equals.shouldBeEqual
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.call
import io.ktor.server.request.receiveText
import io.ktor.server.response.respond
import io.ktor.server.routing.route
import io.ktor.server.testing.testApplication
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.measureTimedValue

class LongPollTest : FunSpec({
    test("test long polling") {
        val redis = RedisServer.newRedisServer().start()
        val jobPersistence = HashMapJobPersistence()
        val dataPersistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
        var jobFramework: kotlinx.coroutines.Job? = null
        testApplication {
            routing {
                route("test") {
                    jobFramework = JobFramework(defaultInstanceName, jobPersistence) {
                        addApi(
                            "J1", this@route, dataPersistence, { TestInput(call.receiveText().toInt(), 2_000) },
                            { call.respond(it.inputValue) }, defaultComputation
                        ) {
                            enableLongPolling({ RedisLongPollManager(redis.lettuceClient) }) {
                                maximumConnectionTimeout = 200.milliseconds
                            }
                        }
                        maintenanceConfig {
                            jobCheckInterval = 5.milliseconds
                        }
                    }
                }
            }

            val uuid = client.post("test/submit") { setBody("5") }.bodyAsText()
            var i = 0
            while (++i <= 11) {
                val (response, duration) = measureTimedValue { client.get("test/poll/$uuid") }
                val result = response.bodyAsText()
                response.status shouldBeEqual HttpStatusCode.OK
                if (result == "TIMEOUT") {
                    duration shouldBeGreaterThan 200.milliseconds
                    duration shouldBeLessThan 1.seconds
                    i shouldBeLessThan 10
                } else {
                    result shouldBeEqual "SUCCESS"
                    break
                }
            }
            i shouldBeLessThan 11
        }
        jobFramework!!.cancelAndJoin()
    }

    test("test custom timeouts") {
        val redis = RedisServer.newRedisServer().start()
        val jobPersistence = HashMapJobPersistence()
        val dataPersistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
        var jobFramework: kotlinx.coroutines.Job? = null
        testApplication {
            routing {
                route("test") {
                    jobFramework = JobFramework(defaultInstanceName, jobPersistence) {
                        addApi(
                            "J1", this@route, dataPersistence, { TestInput(call.receiveText().toInt(), 20_000) },
                            { call.respond(it.inputValue) }, defaultComputation
                        ) {
                            enableLongPolling({ RedisLongPollManager(redis.lettuceClient) }) {
                                maximumConnectionTimeout = 1.seconds
                            }
                        }
                        maintenanceConfig {
                            jobCheckInterval = 5.milliseconds
                        }
                    }
                }
            }

            val uuid = client.post("test/submit") { setBody("5") }.bodyAsText()

            val (response1, duration1) = measureTimedValue { client.get("test/poll/$uuid?timeout=10") }
            duration1 shouldBeGreaterThan 10.milliseconds
            duration1 shouldBeLessThan 100.milliseconds
            response1.bodyAsText() shouldBeEqual "TIMEOUT"

            val (response2, duration2) = measureTimedValue { client.get("test/poll/$uuid?timeout=200") }
            duration2 shouldBeGreaterThan 200.milliseconds
            duration2 shouldBeLessThan 400.milliseconds
            response2.bodyAsText() shouldBeEqual "TIMEOUT"

            val (response3, duration3) = measureTimedValue { client.get("test/poll/$uuid?timeout=2000") }
            // timeout limited by server to 1 second
            duration3 shouldBeGreaterThan 1000.milliseconds
            duration3 shouldBeLessThan 1500.milliseconds
            response3.bodyAsText() shouldBeEqual "TIMEOUT"
        }
        jobFramework!!.cancelAndJoin()
    }

    test("test poll of finished job") {
        val redis = RedisServer.newRedisServer().start()
        val jobPersistence = HashMapJobPersistence()
        val dataPersistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
        var jobFramework: kotlinx.coroutines.Job? = null
        testApplication {
            routing {
                route("test") {
                    jobFramework = JobFramework(defaultInstanceName, jobPersistence) {
                        addApi(
                            "J1", this@route, dataPersistence, { call.receiveText().toInt().let { TestInput(it, 0, it < 0) } },
                            { call.respond(it.inputValue) }, defaultComputation
                        ) {
                            enableLongPolling({ RedisLongPollManager(redis.lettuceClient) }) {
                                maximumConnectionTimeout = 3.minutes
                            }
                        }
                        maintenanceConfig {
                            jobCheckInterval = 5.milliseconds
                        }
                    }
                }
            }
            val successUuid = client.post("test/submit") { setBody("5") }.bodyAsText()
            while (client.get("test/status/$successUuid").bodyAsText() != "SUCCESS") delay(10.milliseconds)
            val (successResponse, successDuration) = measureTimedValue { client.get("test/poll/$successUuid") }
            successDuration shouldBeLessThan 1.seconds
            successResponse.status shouldBeEqual HttpStatusCode.OK
            successResponse.bodyAsText() shouldBeEqual "SUCCESS"

            val failureUuid = client.post("test/submit") { setBody("-1") }.bodyAsText()
            while (client.get("test/status/$failureUuid").bodyAsText() != "FAILURE") delay(10.milliseconds)
            val (failureResponse, failureDuration) = measureTimedValue { client.get("test/poll/$failureUuid") }
            failureDuration shouldBeLessThan 1.seconds
            failureResponse.status shouldBeEqual HttpStatusCode.OK
            failureResponse.bodyAsText() shouldBeEqual "FAILURE"
        }
        jobFramework!!.cancelAndJoin()
    }

    test("test long poll with cancelled job") {
        val redis = RedisServer.newRedisServer().start()
        val jobPersistence = HashMapJobPersistence()
        val dataPersistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
        var jobFramework: kotlinx.coroutines.Job? = null
        testApplication {
            routing {
                route("test") {
                    jobFramework = JobFramework(defaultInstanceName, jobPersistence) {
                        addApi(
                            "J1", this@route, dataPersistence, { TestInput(call.receiveText().toInt(), 2_000) },
                            { call.respond(it.inputValue) }, defaultComputation
                        ) {
                            enableLongPolling({ RedisLongPollManager(redis.lettuceClient) })
                        }
                        enableCancellation {
                            checkInterval = 5.milliseconds
                        }
                        maintenanceConfig {
                            jobCheckInterval = 5.milliseconds
                        }
                    }
                }
            }
            val uuid = client.post("test/submit") { setBody("5") }.bodyAsText()
            val (response, duration) = measureTimedValue { client.get("test/poll/$uuid?timeout=100") }
            duration shouldBeGreaterThan 100.milliseconds
            duration shouldBeLessThan 1.seconds
            response.status shouldBeEqual HttpStatusCode.OK
            response.bodyAsText() shouldBeEqual "TIMEOUT"

            client.post("test/cancel/$uuid").status shouldBeEqual HttpStatusCode.OK
            client.get("test/status/$uuid").bodyAsText() shouldBeEqual "CANCEL_REQUESTED"
            measureTimedValue { client.get("test/poll/$uuid") }.also { (response, duration) ->
                response.status shouldBeEqual HttpStatusCode.OK
                response.bodyAsText() shouldBeEqual "ABORTED"
                duration shouldBeLessThan 200.milliseconds
            }
        }
        jobFramework!!.cancelAndJoin()
    }

    test("test multiple polls on same uuid") {
        val redis = RedisServer.newRedisServer().start()
        val jobPersistence = HashMapJobPersistence()
        val dataPersistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
        var jobFramework: kotlinx.coroutines.Job? = null
        testApplication {
            routing {
                route("test") {
                    jobFramework = JobFramework(defaultInstanceName, jobPersistence) {
                        addApi(
                            "J1", this@route, dataPersistence, { TestInput(call.receiveText().toInt(), 500) },
                            { call.respond(it.inputValue) }, defaultComputation
                        ) {
                            enableLongPolling({ RedisLongPollManager(redis.lettuceClient) }) {
                                maximumConnectionTimeout = 3.minutes
                            }
                        }
                        maintenanceConfig {
                            jobCheckInterval = 5.milliseconds
                        }
                    }
                }
            }
            val uuid = client.post("test/submit") { setBody("5") }.bodyAsText()
            val result0 = async(Dispatchers.IO) { client.get("test/poll/$uuid?timeout=20") }
            val result1 = async(Dispatchers.IO) { client.get("test/poll/$uuid") }
            val result2 = async(Dispatchers.IO) { client.get("test/poll/$uuid?timeout=100") }
            val result3 = async(Dispatchers.IO) { client.get("test/poll/$uuid?timeout=10") }
            result0.await().bodyAsText() shouldBeEqual "TIMEOUT"
            result1.await().bodyAsText() shouldBeEqual "SUCCESS"
            result2.await().bodyAsText() shouldBeEqual "TIMEOUT"
            result3.await().bodyAsText() shouldBeEqual "TIMEOUT"
        }
        jobFramework!!.cancelAndJoin()
    }

    test("test long poll on multiple job types") {
        val redis = RedisServer.newRedisServer().start()
        val jobPersistence = HashMapJobPersistence()
        val dataPersistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
        var jobFramework: kotlinx.coroutines.Job? = null
        testApplication {
            routing {
                jobFramework = JobFramework(defaultInstanceName, jobPersistence) {
                    maintenanceConfig {
                        jobCheckInterval = 5.milliseconds
                    }
                    route("test") {
                        addApi(
                            "J1", this@route, dataPersistence, { TestInput(call.receiveText().toInt(), 500) },
                            { call.respond(it.inputValue) }, defaultComputation
                        ) {
                            enableLongPolling({ RedisLongPollManager(redis.lettuceClient) }) {
                                maximumConnectionTimeout = 3.minutes
                            }
                        }
                    }
                    route("test2") {
                        addApi(
                            "J2", this@route, dataPersistence, { TestInput(call.receiveText().toInt(), 1000) },
                            { call.respond(it.inputValue) }, defaultComputation
                        ) {
                            enableLongPolling({ RedisLongPollManager(redis.lettuceClient) }) {
                                maximumConnectionTimeout = 3.minutes
                            }
                        }
                    }
                }
            }
            val uuid1 = client.post("test/submit") { setBody("5") }.bodyAsText()
            val uuid2 = client.post("test2/submit") { setBody("6") }.bodyAsText()
            val result0 = async(Dispatchers.IO) { client.get("test/poll/$uuid1?timeout=50") }
            val result1 = async(Dispatchers.IO) { client.get("test2/poll/$uuid2?timeout=50") }
            val result2 = async(Dispatchers.IO) { client.get("test/poll/$uuid1") }
            val result3 = async(Dispatchers.IO) { client.get("test2/poll/$uuid2") }
            val result4 = async(Dispatchers.IO) { client.get("test/poll/$uuid1?timeout=10") }
            val result5 = async(Dispatchers.IO) { client.get("test2/poll/$uuid2?timeout=10") }
            result0.await().bodyAsText() shouldBeEqual "TIMEOUT"
            result1.await().bodyAsText() shouldBeEqual "TIMEOUT"
            result4.await().bodyAsText() shouldBeEqual "TIMEOUT"
            result5.await().bodyAsText() shouldBeEqual "TIMEOUT"
            result2.await().bodyAsText() shouldBeEqual "SUCCESS"
            result3.await().bodyAsText() shouldBeEqual "SUCCESS"
        }
        jobFramework!!.cancelAndJoin()
    }

    test("test long poll with multiple uuids") {
        val redis = RedisServer.newRedisServer().start()
        val jobPersistence = HashMapJobPersistence()
        val dataPersistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
        var jobFramework: kotlinx.coroutines.Job? = null
        testApplication {
            routing {
                jobFramework = JobFramework(defaultInstanceName, jobPersistence) {
                    maintenanceConfig {
                        jobCheckInterval = 10.milliseconds
                    }
                    executorConfig {
                        executionCapacityProvider = ExecutionCapacityProvider { ExecutionCapacity.Companion.AcceptingAnyJob }
                    }
                    route("test") {
                        addApi(
                            "J1", this@route, dataPersistence, { call.receiveText().toInt().let { TestInput(it, it) } },
                            { call.respond(it.inputValue) }, defaultComputation
                        ) {
                            enableLongPolling({ RedisLongPollManager(redis.lettuceClient) }) {
                                maximumConnectionTimeout = 3.minutes
                            }
                        }
                    }
                }
            }
            val uuid1 = client.post("test/submit") { setBody("100") }.bodyAsText()
            val uuid2 = client.post("test/submit") { setBody("500") }.bodyAsText()
            val uuid3 = client.post("test/submit") { setBody("200") }.bodyAsText()
            val result2 = async(Dispatchers.IO) { measureTimedValue { client.get("test/poll/$uuid2") } }
            val result1 = async(Dispatchers.IO) { measureTimedValue { client.get("test/poll/$uuid1") } }
            val result3 = async(Dispatchers.IO) { measureTimedValue { client.get("test/poll/$uuid3") } }
            val (response1, duration1) = result1.await()
            val (response2, duration2) = result2.await()
            val (response3, duration3) = result3.await()
            response1.bodyAsText() shouldBeEqual "SUCCESS"
            response2.bodyAsText() shouldBeEqual "SUCCESS"
            response3.bodyAsText() shouldBeEqual "SUCCESS"
            duration1 shouldBeGreaterThan 100.milliseconds
            duration1 shouldBeLessThan 200.milliseconds
            duration2 shouldBeGreaterThan 500.milliseconds
            duration2 shouldBeLessThan 700.milliseconds
            duration3 shouldBeGreaterThan 200.milliseconds
            duration3 shouldBeLessThan 300.milliseconds
        }
        jobFramework!!.cancelAndJoin()
    }
})
