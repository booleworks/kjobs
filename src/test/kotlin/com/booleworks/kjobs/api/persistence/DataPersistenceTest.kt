// SPDX-License-Identifier: MIT
// Copyright 2023-2024 BooleWorks GmbH

package com.booleworks.kjobs.api.persistence

import com.booleworks.kjobs.api.persistence.hashmap.HashMapDataPersistence
import com.booleworks.kjobs.api.persistence.hashmap.HashMapJobPersistence
import com.booleworks.kjobs.api.persistence.redis.DefaultRedisConfig
import com.booleworks.kjobs.api.persistence.redis.RedisConfig
import com.booleworks.kjobs.api.persistence.redis.RedisDataPersistence
import com.booleworks.kjobs.api.persistence.redis.RedisJobPersistence
import com.booleworks.kjobs.common.TestInput
import com.booleworks.kjobs.common.TestResult
import com.booleworks.kjobs.common.expectSuccess
import com.booleworks.kjobs.common.lettuceClient
import com.booleworks.kjobs.data.PersistenceAccessResult
import com.booleworks.kjobs.data.uuidNotFound
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.github.fppt.jedismock.RedisServer
import io.kotest.core.spec.style.FunSpec
import io.kotest.engine.runBlocking
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.equals.shouldBeEqual
import io.lettuce.core.codec.ByteArrayCodec
import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream

val redisConfigWithCompression = object : DefaultRedisConfig() {
    override val useCompression = true
}

class DataPersistenceTest : FunSpec({

    fun testPersistences(testName: String, config: RedisConfig = DefaultRedisConfig(), block: suspend (DataPersistence<MyInput, MyResult>) -> Unit) = runBlocking {
        val hashMapJobPersistence = HashMapJobPersistence()
        test("HashMap: $testName") { block(HashMapDataPersistence(hashMapJobPersistence)) }
        val redis = RedisServer.newRedisServer().start()
        test("Redis: $testName") {
            block(
                RedisDataPersistence(
                    redis.lettuceClient,
                    { jacksonObjectMapper().writeValueAsBytes(it) },
                    { jacksonObjectMapper().writeValueAsBytes(it) },
                    { jacksonObjectMapper().readValue(it) },
                    { jacksonObjectMapper().readValue(it) },
                    config
                )
            )
        }
    }

    testPersistences("test persist input") { dataPersistence ->
        val inputs = myInputGenerator()
        dataPersistence.fetchInput("42") shouldBeEqual PersistenceAccessResult.uuidNotFound("42")
        dataPersistence.dataTransaction {
            inputs.forEach { persistInput(newJob(it.first), it.second).expectSuccess() }
        }
        dataPersistence.fetchInput("42").expectSuccess() shouldBeEqual inputs[0].second
        dataPersistence.fetchInput("43").expectSuccess() shouldBeEqual inputs[1].second
        dataPersistence.fetchInput("44").expectSuccess() shouldBeEqual inputs[2].second
        dataPersistence.fetchInput("45").expectSuccess() shouldBeEqual inputs[3].second
        dataPersistence.fetchInput("46").expectSuccess() shouldBeEqual inputs[4].second
        dataPersistence.fetchInput("47") shouldBeEqual PersistenceAccessResult.uuidNotFound("47")
    }

    testPersistences("test persist result") { dataPersistence ->
        val results = myResultGenerator()
        dataPersistence.fetchResult("42") shouldBeEqual PersistenceAccessResult.uuidNotFound("42")
        dataPersistence.dataTransaction {
            results.forEach { persistOrUpdateResult(newJob(it.first), it.second).expectSuccess() }
        }
        dataPersistence.fetchResult("42").expectSuccess() shouldBeEqual results[0].second
        dataPersistence.fetchResult("43").expectSuccess() shouldBeEqual results[1].second
        dataPersistence.fetchResult("44").expectSuccess() shouldBeEqual results[2].second
        dataPersistence.fetchResult("45").expectSuccess() shouldBeEqual results[3].second
        dataPersistence.fetchResult("46").expectSuccess() shouldBeEqual results[4].second
        dataPersistence.fetchResult("47") shouldBeEqual PersistenceAccessResult.uuidNotFound("47")

        val newResult = MyResult(1, "2", listOf(3.0), setOf(MySubObject(4, "5")))
        dataPersistence.dataTransaction {
            persistOrUpdateResult(newJob("44"), newResult)
            persistOrUpdateResult(newJob("46"), newResult)
        }
        dataPersistence.fetchResult("42").expectSuccess() shouldBeEqual results[0].second
        dataPersistence.fetchResult("43").expectSuccess() shouldBeEqual results[1].second
        dataPersistence.fetchResult("44").expectSuccess() shouldBeEqual newResult
        dataPersistence.fetchResult("45").expectSuccess() shouldBeEqual results[3].second
        dataPersistence.fetchResult("46").expectSuccess() shouldBeEqual newResult
    }

    testPersistences("test persist failure") { dataPersistence ->
        dataPersistence.fetchFailure("42") shouldBeEqual PersistenceAccessResult.uuidNotFound("42")
        dataPersistence.dataTransaction {
            persistOrUpdateFailure(newJob("42"), "").expectSuccess()
            persistOrUpdateFailure(newJob("43"), "something strange happened").expectSuccess()
            persistOrUpdateFailure(newJob("44"), "out of memory").expectSuccess()
            persistOrUpdateFailure(newJob("45"), "too much work").expectSuccess()
        }
        dataPersistence.fetchFailure("42").expectSuccess() shouldBeEqual ""
        dataPersistence.fetchFailure("43").expectSuccess() shouldBeEqual "something strange happened"
        dataPersistence.fetchFailure("44").expectSuccess() shouldBeEqual "out of memory"
        dataPersistence.fetchFailure("45").expectSuccess() shouldBeEqual "too much work"
        dataPersistence.fetchFailure("47") shouldBeEqual PersistenceAccessResult.uuidNotFound("47")

        dataPersistence.dataTransaction {
            persistOrUpdateFailure(newJob("44"), "hello world")
            persistOrUpdateFailure(newJob("45"), "")
        }
        dataPersistence.fetchFailure("42").expectSuccess() shouldBeEqual ""
        dataPersistence.fetchFailure("43").expectSuccess() shouldBeEqual "something strange happened"
        dataPersistence.fetchFailure("44").expectSuccess() shouldBeEqual "hello world"
        dataPersistence.fetchFailure("45").expectSuccess() shouldBeEqual ""
    }

    testPersistences("test wrong result type") { dataPersistence ->
        dataPersistence.dataTransaction {
            persistOrUpdateResult(newJob("42"), myResultGenerator()[0].second).expectSuccess()
            persistOrUpdateFailure(newJob("43"), "something strange happened").expectSuccess()
        }
        dataPersistence.fetchFailure("42") shouldBeEqual PersistenceAccessResult.uuidNotFound("42")
        dataPersistence.fetchResult("43") shouldBeEqual PersistenceAccessResult.uuidNotFound("43")
    }

    test("HashMap: test delete") {
        val jobPersistence = HashMapJobPersistence()
        val testInputPersistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
        val myInputPersistence = HashMapDataPersistence<MyInput, MyResult>(jobPersistence)
        val anotherInputPersistence = HashMapDataPersistence<AnotherInput, AnotherResult>(jobPersistence)
        val persistenceMap = mapOf("test" to testInputPersistence, "my" to myInputPersistence, "another" to anotherInputPersistence)

        val job42 = newJob("42").copy(type = "test")
        testInputPersistence.persistJob(job42).expectSuccess()
        testInputPersistence.persistInput(job42, TestInput(42)).expectSuccess()
        testInputPersistence.persistOrUpdateResult(job42, TestResult(42)).expectSuccess()
        val job43 = newJob("43").copy(type = "test")
        testInputPersistence.persistJob(job43).expectSuccess()
        testInputPersistence.persistInput(job43, TestInput(43)).expectSuccess()
        testInputPersistence.persistOrUpdateFailure(job43, "FAIL").expectSuccess()
        val job44 = newJob("44").copy(type = "my")
        myInputPersistence.persistJob(job44).expectSuccess()
        myInputPersistence.persistInput(job44, myInputGenerator()[1].second).expectSuccess()
        myInputPersistence.persistOrUpdateResult(job44, myResultGenerator()[2].second).expectSuccess()
        val job45 = newJob("45").copy(type = "my")
        myInputPersistence.persistJob(job45).expectSuccess()
        myInputPersistence.persistInput(job45, myInputGenerator()[2].second).expectSuccess()
        myInputPersistence.persistOrUpdateFailure(job45, "FAIL").expectSuccess()
        val job46 = newJob("46").copy(type = "another")
        anotherInputPersistence.persistJob(job46).expectSuccess()
        anotherInputPersistence.persistInput(job46, AnotherInput(46)).expectSuccess()
        anotherInputPersistence.persistOrUpdateResult(job46, AnotherResult(46)).expectSuccess()
        val job47 = newJob("47").copy(type = "another")
        anotherInputPersistence.persistJob(job47).expectSuccess()
        anotherInputPersistence.persistInput(job47, AnotherInput(47)).expectSuccess()
        anotherInputPersistence.persistOrUpdateFailure(job47, "FAIL").expectSuccess()

        jobPersistence.deleteForUuid("41", persistenceMap).expectSuccess()
        jobPersistence.deleteForUuid("42", persistenceMap).expectSuccess()
        jobPersistence.deleteForUuid("43", persistenceMap).expectSuccess()
        jobPersistence.deleteForUuid("44", persistenceMap).expectSuccess()
        jobPersistence.deleteForUuid("45", persistenceMap).expectSuccess()
        jobPersistence.deleteForUuid("46", persistenceMap).expectSuccess()
        // duplicate deletion should still be successful
        jobPersistence.deleteForUuid("46", persistenceMap).expectSuccess()
        testInputPersistence.fetchJob("42") shouldBeEqual PersistenceAccessResult.uuidNotFound("42")
        testInputPersistence.fetchInput("42") shouldBeEqual PersistenceAccessResult.uuidNotFound("42")
        testInputPersistence.fetchResult("42") shouldBeEqual PersistenceAccessResult.uuidNotFound("42")
        testInputPersistence.fetchJob("43") shouldBeEqual PersistenceAccessResult.uuidNotFound("43")
        testInputPersistence.fetchInput("43") shouldBeEqual PersistenceAccessResult.uuidNotFound("43")
        testInputPersistence.fetchFailure("43") shouldBeEqual PersistenceAccessResult.uuidNotFound("43")
        myInputPersistence.fetchJob("44") shouldBeEqual PersistenceAccessResult.uuidNotFound("44")
        myInputPersistence.fetchInput("44") shouldBeEqual PersistenceAccessResult.uuidNotFound("44")
        myInputPersistence.fetchResult("44") shouldBeEqual PersistenceAccessResult.uuidNotFound("44")
        myInputPersistence.fetchJob("45") shouldBeEqual PersistenceAccessResult.uuidNotFound("45")
        myInputPersistence.fetchInput("45") shouldBeEqual PersistenceAccessResult.uuidNotFound("45")
        myInputPersistence.fetchFailure("45") shouldBeEqual PersistenceAccessResult.uuidNotFound("45")
        anotherInputPersistence.fetchJob("46") shouldBeEqual PersistenceAccessResult.uuidNotFound("46")
        anotherInputPersistence.fetchInput("46") shouldBeEqual PersistenceAccessResult.uuidNotFound("46")
        anotherInputPersistence.fetchResult("46") shouldBeEqual PersistenceAccessResult.uuidNotFound("46")
        anotherInputPersistence.fetchJob("47").expectSuccess() shouldBeEqual job47
        anotherInputPersistence.fetchInput("47").expectSuccess() shouldBeEqual AnotherInput(47)
        anotherInputPersistence.fetchFailure("47").expectSuccess() shouldBeEqual "FAIL"
        jobPersistence.deleteForUuid("47", persistenceMap).expectSuccess()
        anotherInputPersistence.fetchJob("47") shouldBeEqual PersistenceAccessResult.uuidNotFound("47")
        anotherInputPersistence.fetchInput("47") shouldBeEqual PersistenceAccessResult.uuidNotFound("47")
        anotherInputPersistence.fetchFailure("47") shouldBeEqual PersistenceAccessResult.uuidNotFound("47")
    }

    test("Redis: test delete") {
        val redis = RedisServer.newRedisServer().start()
        val redisClient = redis.lettuceClient
        val jobPersistence = RedisJobPersistence(redisClient)
        val testInputPersistence = RedisDataPersistence<TestInput, TestResult>(
            redisClient,
            { jacksonObjectMapper().writeValueAsBytes(it) },
            { jacksonObjectMapper().writeValueAsBytes(it) },
            { jacksonObjectMapper().readValue(it) },
            { jacksonObjectMapper().readValue(it) }
        )
        val myInputPersistence = RedisDataPersistence<MyInput, MyResult>(
            redisClient,
            { jacksonObjectMapper().writeValueAsBytes(it) },
            { jacksonObjectMapper().writeValueAsBytes(it) },
            { jacksonObjectMapper().readValue(it) },
            { jacksonObjectMapper().readValue(it) }
        )
        val anotherInputPersistence = RedisDataPersistence<AnotherInput, AnotherResult>(
            redisClient,
            { jacksonObjectMapper().writeValueAsBytes(it) },
            { jacksonObjectMapper().writeValueAsBytes(it) },
            { jacksonObjectMapper().readValue(it) },
            { jacksonObjectMapper().readValue(it) }
        )
        val persistenceMap = mapOf("test" to testInputPersistence, "my" to myInputPersistence, "another" to anotherInputPersistence)

        testInputPersistence.dataTransaction {
            val job42 = newJob("42").copy(type = "test")
            persistJob(job42).expectSuccess()
            persistInput(job42, TestInput(42)).expectSuccess()
            persistOrUpdateResult(job42, TestResult(42)).expectSuccess()
            val job43 = newJob("43").copy(type = "test")
            persistJob(job43).expectSuccess()
            persistInput(job43, TestInput(43)).expectSuccess()
            persistOrUpdateFailure(job43, "FAIL").expectSuccess()
        }
        myInputPersistence.dataTransaction {
            val job44 = newJob("44").copy(type = "my")
            persistJob(job44).expectSuccess()
            persistInput(job44, myInputGenerator()[1].second).expectSuccess()
            persistOrUpdateResult(job44, myResultGenerator()[2].second).expectSuccess()
            val job45 = newJob("45").copy(type = "my")
            persistJob(job45).expectSuccess()
            persistInput(job45, myInputGenerator()[2].second).expectSuccess()
            persistOrUpdateFailure(job45, "FAIL").expectSuccess()
        }
        anotherInputPersistence.dataTransaction {
            val job46 = newJob("46").copy(type = "another")
            persistJob(job46).expectSuccess()
            persistInput(job46, AnotherInput(46)).expectSuccess()
            persistOrUpdateResult(job46, AnotherResult(46)).expectSuccess()
            val job47 = newJob("47").copy(type = "another")
            persistJob(job47).expectSuccess()
            persistInput(job47, AnotherInput(47)).expectSuccess()
            persistOrUpdateFailure(job47, "FAIL").expectSuccess()
        }
        jobPersistence.transaction {
            deleteForUuid("41", persistenceMap).expectSuccess()
            deleteForUuid("42", persistenceMap).expectSuccess()
            deleteForUuid("43", persistenceMap).expectSuccess()
            deleteForUuid("44", persistenceMap).expectSuccess()
            deleteForUuid("45", persistenceMap).expectSuccess()
            deleteForUuid("46", persistenceMap).expectSuccess()
            // duplicate deletion should still be successful
            deleteForUuid("46", persistenceMap).expectSuccess()
        }
        testInputPersistence.fetchJob("42") shouldBeEqual PersistenceAccessResult.uuidNotFound("42")
        testInputPersistence.fetchInput("42") shouldBeEqual PersistenceAccessResult.uuidNotFound("42")
        testInputPersistence.fetchResult("42") shouldBeEqual PersistenceAccessResult.uuidNotFound("42")
        testInputPersistence.fetchJob("43") shouldBeEqual PersistenceAccessResult.uuidNotFound("43")
        testInputPersistence.fetchInput("43") shouldBeEqual PersistenceAccessResult.uuidNotFound("43")
        testInputPersistence.fetchFailure("43") shouldBeEqual PersistenceAccessResult.uuidNotFound("43")
        myInputPersistence.fetchJob("44") shouldBeEqual PersistenceAccessResult.uuidNotFound("44")
        myInputPersistence.fetchInput("44") shouldBeEqual PersistenceAccessResult.uuidNotFound("44")
        myInputPersistence.fetchResult("44") shouldBeEqual PersistenceAccessResult.uuidNotFound("44")
        myInputPersistence.fetchJob("45") shouldBeEqual PersistenceAccessResult.uuidNotFound("45")
        myInputPersistence.fetchInput("45") shouldBeEqual PersistenceAccessResult.uuidNotFound("45")
        myInputPersistence.fetchFailure("45") shouldBeEqual PersistenceAccessResult.uuidNotFound("45")
        anotherInputPersistence.fetchJob("46") shouldBeEqual PersistenceAccessResult.uuidNotFound("46")
        anotherInputPersistence.fetchInput("46") shouldBeEqual PersistenceAccessResult.uuidNotFound("46")
        anotherInputPersistence.fetchResult("46") shouldBeEqual PersistenceAccessResult.uuidNotFound("46")
        anotherInputPersistence.fetchJob("47").expectSuccess() shouldBeEqual newJob("47").copy(type = "another")
        anotherInputPersistence.fetchInput("47").expectSuccess() shouldBeEqual AnotherInput(47)
        anotherInputPersistence.fetchFailure("47").expectSuccess() shouldBeEqual "FAIL"
        jobPersistence.transaction { deleteForUuid("47", persistenceMap).expectSuccess() }
        anotherInputPersistence.fetchJob("47") shouldBeEqual PersistenceAccessResult.uuidNotFound("47")
        anotherInputPersistence.fetchInput("47") shouldBeEqual PersistenceAccessResult.uuidNotFound("47")
        anotherInputPersistence.fetchFailure("47") shouldBeEqual PersistenceAccessResult.uuidNotFound("47")
    }

    test("Redis: test persist input with compression") {
        val redis = RedisServer.newRedisServer().start()
        val dataPersistence = RedisDataPersistence<MyInput, MyResult>(
            redis.lettuceClient,
            { jacksonObjectMapper().writeValueAsBytes(it) },
            { jacksonObjectMapper().writeValueAsBytes(it) },
            { jacksonObjectMapper().readValue(it) },
            { jacksonObjectMapper().readValue(it) },
            redisConfigWithCompression
        )
        val inputs = myInputGenerator()
        dataPersistence.fetchInput("42") shouldBeEqual PersistenceAccessResult.uuidNotFound("42")
        dataPersistence.dataTransaction {
            inputs.forEach { persistInput(newJob(it.first), it.second).expectSuccess() }
        }
        redis.lettuceClient.connect(ByteArrayCodec.INSTANCE).sync().get("input:42".toByteArray()).toList() shouldContainExactly compressToByteList(inputs[0].second)
        dataPersistence.fetchInput("42").expectSuccess() shouldBeEqual inputs[0].second
        dataPersistence.fetchInput("43").expectSuccess() shouldBeEqual inputs[1].second
        dataPersistence.fetchInput("44").expectSuccess() shouldBeEqual inputs[2].second
        dataPersistence.fetchInput("45").expectSuccess() shouldBeEqual inputs[3].second
        dataPersistence.fetchInput("46").expectSuccess() shouldBeEqual inputs[4].second
        dataPersistence.fetchInput("47") shouldBeEqual PersistenceAccessResult.uuidNotFound("47")
    }

    test("Redis: test persist result with compression") {
        val redis = RedisServer.newRedisServer().start()
        val dataPersistence = RedisDataPersistence<MyInput, MyResult>(
            redis.lettuceClient,
            { jacksonObjectMapper().writeValueAsBytes(it) },
            { jacksonObjectMapper().writeValueAsBytes(it) },
            { jacksonObjectMapper().readValue(it) },
            { jacksonObjectMapper().readValue(it) },
            redisConfigWithCompression
        )
        val results = myResultGenerator()
        dataPersistence.fetchResult("42") shouldBeEqual PersistenceAccessResult.uuidNotFound("42")
        dataPersistence.dataTransaction {
            results.forEach { persistOrUpdateResult(newJob(it.first), it.second).expectSuccess() }
        }
        redis.lettuceClient.connect(ByteArrayCodec.INSTANCE).sync().get("result:44".toByteArray()).toList() shouldContainExactly compressToByteList(results[2].second)
        dataPersistence.fetchResult("42").expectSuccess() shouldBeEqual results[0].second
        dataPersistence.fetchResult("43").expectSuccess() shouldBeEqual results[1].second
        dataPersistence.fetchResult("44").expectSuccess() shouldBeEqual results[2].second
        dataPersistence.fetchResult("45").expectSuccess() shouldBeEqual results[3].second
        dataPersistence.fetchResult("46").expectSuccess() shouldBeEqual results[4].second
        dataPersistence.fetchResult("47") shouldBeEqual PersistenceAccessResult.uuidNotFound("47")

        val newResult = MyResult(1, "2", listOf(3.0), setOf(MySubObject(4, "5")))
        dataPersistence.dataTransaction {
            persistOrUpdateResult(newJob("44"), newResult)
            persistOrUpdateResult(newJob("46"), newResult)
        }
        redis.lettuceClient.connect(ByteArrayCodec.INSTANCE).sync().get("result:44".toByteArray()).toList() shouldContainExactly compressToByteList(newResult)
        dataPersistence.fetchResult("42").expectSuccess() shouldBeEqual results[0].second
        dataPersistence.fetchResult("43").expectSuccess() shouldBeEqual results[1].second
        dataPersistence.fetchResult("44").expectSuccess() shouldBeEqual newResult
        dataPersistence.fetchResult("45").expectSuccess() shouldBeEqual results[3].second
        dataPersistence.fetchResult("46").expectSuccess() shouldBeEqual newResult
    }
})

private fun myInputGenerator(): List<Pair<String, MyInput>> = listOf(
    "42" to MyInput(1, "A", listOf(42.42, 1.2, 3.14), mySubObjectGenerator().take(3).toSet()),
    "43" to MyInput(null, "", listOf(), setOf()),
    "44" to MyInput(Int.MIN_VALUE, "42", listOf(1.23), mySubObjectGenerator().toSet() + null),
    "45" to MyInput(Int.MAX_VALUE, "Hello World", listOf(42.0, Double.MAX_VALUE, 42.0), setOf(null)),
    "46" to MyInput(-42, "Foo  ", listOf(Double.MIN_VALUE, Double.NaN), mySubObjectGenerator().toSet()),
)

private fun myResultGenerator(): List<Pair<String, MyResult>> = listOf(
    "42" to MyResult(1, "A", listOf(42.42, 1.2, 3.14), mySubObjectGenerator().take(3).toSet()),
    "43" to MyResult(0, null, null, setOf()),
    "44" to MyResult(Int.MIN_VALUE, "", null, mySubObjectGenerator().toSet()),
    "45" to MyResult(Int.MAX_VALUE, "Hello World", listOf(42.0, Double.MAX_VALUE, 42.0), mySubObjectGenerator().take(1).toSet()),
    "46" to MyResult(-42, "Foo  ", listOf(Double.MIN_VALUE, Double.NaN), mySubObjectGenerator().toSet()),
)

private fun mySubObjectGenerator(): List<MySubObject> = listOf(
    MySubObject(42, "X"),
    MySubObject(0, null),
    MySubObject(0, "123"),
    MySubObject(-700, ""),
    MySubObject(443278034, "Hello World!"),
    MySubObject(Int.MIN_VALUE, null),
)

private data class MySubObject(
    val x: Int,
    val y: String?,
)

private data class MyInput(
    val a: Int?,
    val b: String,
    val c: List<Double>,
    val d: Set<MySubObject?>,
)

private data class MyResult(
    val a: Int,
    val b: String?,
    val c: List<Double>?,
    val d: Set<MySubObject>,
)

private data class AnotherInput(val a: Int)
private data class AnotherResult(val a: Int)

private fun compressToByteList(input: Any) =
    ByteArrayOutputStream().also { GZIPOutputStream(it).use { gz -> gz.write(jacksonObjectMapper().writeValueAsBytes(input)) } }.toByteArray().toList()
