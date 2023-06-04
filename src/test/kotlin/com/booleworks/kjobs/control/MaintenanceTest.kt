// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.control

import com.booleworks.kjobs.api.JobFrameworkTestingMode
import com.booleworks.kjobs.api.persistence.newJob
import com.booleworks.kjobs.common.TestInput
import com.booleworks.kjobs.common.TestResult
import com.booleworks.kjobs.common.defaultJobType
import com.booleworks.kjobs.common.expectSuccess
import com.booleworks.kjobs.common.newRedisPersistence
import com.booleworks.kjobs.data.Heartbeat
import com.booleworks.kjobs.data.Job
import com.booleworks.kjobs.data.JobStatus
import com.booleworks.kjobs.data.JobStatus.*
import com.booleworks.kjobs.data.PersistenceAccessResult
import com.booleworks.kjobs.data.uuidNotFound
import com.github.fppt.jedismock.RedisServer
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.equals.shouldBeEqual
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.delay
import redis.clients.jedis.JedisPool
import java.time.LocalDateTime
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

class MaintenanceTest : FunSpec({

    test("test update heartbeat") {
        val redis = RedisServer.newRedisServer().start()
        val persistence = newRedisPersistence<TestInput, TestResult>(redis)
        val testingMode = JobFrameworkTestingMode("I1", persistence, this, false) {
            addJob(defaultJobType, persistence, { _, _ -> ComputationResult.Success(TestResult(42)) }) {}
        }

        val directCall = { instance: String -> suspend { Maintenance.updateHeartbeat(persistence, instance) } }
        val testingCall = { instance: String -> suspend { testingMode.updateHeartbeat(instance) } }

        for (method in listOf(directCall, testingCall)) {
            JedisPool(redis.host, redis.bindPort).resource.use { it.flushDB() }
            persistence.fetchHeartbeats(LocalDateTime.now().minusDays(10)).expectSuccess() shouldHaveSize 0
            val since = LocalDateTime.now()
            method("I1")()
            method("I2")()
            method("I3")()
            persistence.fetchHeartbeats(since).expectSuccess().map { it.instanceName } shouldContainExactlyInAnyOrder listOf("I1", "I2", "I3")
            method("I1")()
            delay(5.milliseconds)
            val since2 = LocalDateTime.now()
            method("I2")()
            method("I3")()
            persistence.fetchHeartbeats(since2).expectSuccess().map { it.instanceName } shouldContainExactlyInAnyOrder listOf("I2", "I3")
        }
    }

    test("test check for cancellation") {
        val redis = RedisServer.newRedisServer().start()
        val persistence = newRedisPersistence<TestInput, TestResult>(redis)
        val testingMode = JobFrameworkTestingMode("I1", persistence, this, false) {
            addJob(defaultJobType, persistence, { _, _ -> ComputationResult.Success(TestResult(42)) }) {}
        }

        val directCall = suspend { Maintenance.checkForCancellations(persistence) }
        val testingCall = suspend { testingMode.checkForCancellations() }

        for (method in listOf(directCall, testingCall)) {
            Maintenance.jobsToBeCancelled = setOf()
            JedisPool(redis.host, redis.bindPort).resource.use { it.flushDB() }
            persistence.transaction {
                persistJob(newJob("42", status = CANCEL_REQUESTED))
                persistJob(newJob("43"))
                persistJob(newJob("44", status = CANCEL_REQUESTED))
                persistJob(newJob("45", status = CANCELLED))
            }
            Maintenance.jobsToBeCancelled shouldHaveSize 0
            method()
            Maintenance.jobsToBeCancelled shouldContainExactlyInAnyOrder listOf("42", "44")
            persistence.transaction {
                persistJob(newJob("44", status = CANCELLED))
                persistJob(newJob("46", status = CANCEL_REQUESTED))
                persistJob(newJob("47", status = CANCEL_REQUESTED))
            }
            method()
            Maintenance.jobsToBeCancelled shouldContainExactlyInAnyOrder listOf("42", "46", "47")
            persistence.transaction {
                persistJob(newJob("42", status = CANCELLED))
                persistJob(newJob("46", status = CREATED))
                persistJob(newJob("47", status = RUNNING))
            }
            method()
            Maintenance.jobsToBeCancelled shouldHaveSize 0
        }
    }

    test("test restart jobs from dead instances") {
        val redis = RedisServer.newRedisServer().start()
        val interval = 300.minutes
        val persistence = newRedisPersistence<TestInput, TestResult>(redis)
        val testingMode = JobFrameworkTestingMode("I1", persistence, this, false) {
            addJob(defaultJobType, persistence, { _, _ -> ComputationResult.Success(TestResult(42)) }) {}
            addJob("other", persistence, { _, _ -> ComputationResult.Success(TestResult(42)) }) { jobConfig { maxRestarts = 2 } }
            maintenanceConfig { heartbeatInterval = interval }
        }

        val directCall = suspend {
            Maintenance.restartJobsFromDeadInstances(
                persistence,
                mapOf(defaultJobType to persistence, "other" to persistence),
                interval,
                mapOf(defaultJobType to 3, "other" to 2)
            )
        }
        val testingCall = suspend { testingMode.restartJobsFromDeadInstances() }

        for (method in listOf(directCall, testingCall)) {
            JedisPool(redis.host, redis.bindPort).resource.use { it.flushDB() }

            persistence.transaction {
                persistJob(newJob("42", status = RUNNING, executingInstance = "I2", startedAt = LocalDateTime.now(), timeout = LocalDateTime.now().plusDays(1)))
                persistJob(newJob("43", status = SUCCESS, executingInstance = "I2", startedAt = LocalDateTime.now(), timeout = LocalDateTime.now().plusDays(1)))
                persistJob(newJob("44", status = RUNNING, executingInstance = "I1", startedAt = LocalDateTime.now(), timeout = LocalDateTime.now().plusDays(1)))
                persistJob(newJob("45", status = RUNNING, executingInstance = "I3", startedAt = LocalDateTime.now(), timeout = LocalDateTime.now().plusDays(1)))
                persistJob(
                    newJob(
                        "46", status = RUNNING, executingInstance = "I4",
                        startedAt = LocalDateTime.now(), timeout = LocalDateTime.now().plusDays(1), numRestarts = 2
                    )
                )
                persistJob(
                    newJob(
                        "47", status = RUNNING, executingInstance = "I4",
                        startedAt = LocalDateTime.now(), timeout = LocalDateTime.now().plusDays(1), numRestarts = 3
                    )
                )
                persistJob(
                    newJob(
                        "48", jobType = "other", status = RUNNING, executingInstance = "I4",
                        startedAt = LocalDateTime.now(), timeout = LocalDateTime.now().plusDays(1), numRestarts = 2
                    )
                )
                updateHeartbeat(Heartbeat("I1", LocalDateTime.now().minus(interval.times(2.09).toJavaDuration())))
                updateHeartbeat(Heartbeat("I2", LocalDateTime.now().minus(interval.times(2.11).toJavaDuration())))
                updateHeartbeat(Heartbeat("I3", LocalDateTime.now().minus(1.seconds.toJavaDuration())))
            }
            method()
            persistence.fetchJob("42").expectSuccess().also {
                it.status shouldBe CREATED
                it.executingInstance.shouldBeNull()
                it.startedAt.shouldBeNull()
                it.timeout.shouldBeNull()
                it.numRestarts shouldBe 1
            }
            persistence.fetchJob("43").expectSuccess().also {
                it.status shouldBe SUCCESS
                it.executingInstance.shouldNotBeNull() shouldBeEqual "I2"
                it.startedAt.shouldNotBeNull()
                it.timeout.shouldNotBeNull()
                it.numRestarts shouldBe 0
            }
            persistence.fetchJob("44").expectSuccess().also {
                it.status shouldBe RUNNING
                it.executingInstance.shouldNotBeNull() shouldBeEqual "I1"
                it.startedAt.shouldNotBeNull()
                it.timeout.shouldNotBeNull()
                it.numRestarts shouldBe 0
            }
            persistence.fetchJob("45").expectSuccess().also {
                it.status shouldBe RUNNING
                it.executingInstance.shouldNotBeNull() shouldBeEqual "I3"
                it.startedAt.shouldNotBeNull()
                it.timeout.shouldNotBeNull()
                it.numRestarts shouldBe 0
            }
            persistence.fetchJob("46").expectSuccess().also {
                it.status shouldBe CREATED
                it.executingInstance.shouldBeNull()
                it.startedAt.shouldBeNull()
                it.timeout.shouldBeNull()
                it.numRestarts shouldBe 3
            }
            persistence.fetchJob("47").expectSuccess().also {
                it.status shouldBe FAILURE
                it.executingInstance.shouldNotBeNull() shouldBeEqual "I4"
                it.startedAt.shouldNotBeNull()
                it.timeout.shouldNotBeNull()
                it.numRestarts shouldBe 3
            }
            persistence.fetchFailure("47").expectSuccess() shouldBeEqual "The job was aborted because it exceeded the maximum number of 3 restarts"
            persistence.fetchFailure("48").expectSuccess() shouldBeEqual "The job was aborted because it exceeded the maximum number of 2 restarts"
        }
    }

    test("test delete old jobs") {
        val redis = RedisServer.newRedisServer().start()
        val interval = 300.minutes
        val persistence = newRedisPersistence<TestInput, TestResult>(redis)
        val testingMode = JobFrameworkTestingMode("I1", persistence, this, false) {
            addJob(defaultJobType, persistence, { _, _ -> ComputationResult.Success(TestResult(42)) }) {}
            maintenanceConfig { deleteOldJobsAfter = interval }
        }

        val directCall = suspend { Maintenance.deleteOldJobs(persistence, interval, mapOf(defaultJobType to persistence)) }
        val testingCall = suspend { testingMode.deleteOldJobs() }

        for (method in listOf(directCall, testingCall)) {
            JedisPool(redis.host, redis.bindPort).resource.use { it.flushDB() }

            persistence.dataTransaction {
                persistJob(newJob("42", status = RUNNING, finishedAt = LocalDateTime.now().minus(interval.toJavaDuration())))
                persistJob(newJob("43", status = CREATED, finishedAt = LocalDateTime.now().minus(interval.toJavaDuration())))
                persistJob(newJob("44", status = CANCEL_REQUESTED, finishedAt = LocalDateTime.now().minus(interval.toJavaDuration())))
                persistJob(newJob("45", status = CANCELLED, finishedAt = LocalDateTime.now().minus(interval.toJavaDuration())))
                persistJob(newJob("46", status = SUCCESS, finishedAt = LocalDateTime.now().minus(interval.toJavaDuration())))
                persistJob(newJob("47", status = FAILURE, finishedAt = LocalDateTime.now().minus(interval.toJavaDuration())))
                persistJob(newJob("48", status = SUCCESS, finishedAt = LocalDateTime.now().minus(interval.toJavaDuration()).plusSeconds(10)))
                persistJob(newJob("49", status = SUCCESS, finishedAt = LocalDateTime.now().minus(interval.toJavaDuration()).minusSeconds(10)))
                persistJob(newJob("50", status = SUCCESS, finishedAt = LocalDateTime.now().minus(interval.toJavaDuration()).plusSeconds(10)))
                persistJob(newJob("51", status = SUCCESS, finishedAt = LocalDateTime.now().minus(interval.toJavaDuration()).minusSeconds(10)))
                persistInput(newJob("42"), TestInput(42))
                persistInput(newJob("43"), TestInput(43))
                persistInput(newJob("44"), TestInput(44))
                persistInput(newJob("45"), TestInput(45))
                persistInput(newJob("46"), TestInput(46))
                persistInput(newJob("47"), TestInput(47))
                persistInput(newJob("48"), TestInput(48))
                persistInput(newJob("49"), TestInput(49))
                persistInput(newJob("50"), TestInput(50))
                persistInput(newJob("51"), TestInput(51))
                persistOrUpdateResult(newJob("42"), TestResult(42))
                persistOrUpdateResult(newJob("43"), TestResult(43))
                persistOrUpdateResult(newJob("44"), TestResult(44))
                persistOrUpdateResult(newJob("45"), TestResult(45))
                persistOrUpdateResult(newJob("46"), TestResult(46))
                persistOrUpdateResult(newJob("47"), TestResult(47))
                persistOrUpdateResult(newJob("48"), TestResult(48))
                persistOrUpdateFailure(newJob("49"), "Some error")
                persistOrUpdateFailure(newJob("50"), "Some error")
                persistOrUpdateFailure(newJob("51"), "Some error")
            }
            method()
            persistence.fetchJob("42").expectSuccess()
            persistence.fetchJob("43").expectSuccess()
            persistence.fetchJob("44").expectSuccess()
            persistence.fetchJob("45").expectSuccess()
            persistence.fetchJob("46") shouldBeEqual PersistenceAccessResult.uuidNotFound("46")
            persistence.fetchJob("47") shouldBeEqual PersistenceAccessResult.uuidNotFound("47")
            persistence.fetchJob("48").expectSuccess()
            persistence.fetchJob("49") shouldBeEqual PersistenceAccessResult.uuidNotFound("49")
            persistence.fetchJob("50").expectSuccess()
            persistence.fetchJob("51") shouldBeEqual PersistenceAccessResult.uuidNotFound("51")
            persistence.fetchInput("42").expectSuccess()
            persistence.fetchInput("43").expectSuccess()
            persistence.fetchInput("44").expectSuccess()
            persistence.fetchInput("45").expectSuccess()
            persistence.fetchInput("46") shouldBeEqual PersistenceAccessResult.uuidNotFound("46")
            persistence.fetchInput("47") shouldBeEqual PersistenceAccessResult.uuidNotFound("47")
            persistence.fetchInput("48").expectSuccess()
            persistence.fetchInput("49") shouldBeEqual PersistenceAccessResult.uuidNotFound("49")
            persistence.fetchInput("50").expectSuccess()
            persistence.fetchInput("51") shouldBeEqual PersistenceAccessResult.uuidNotFound("51")
            persistence.fetchResult("42").expectSuccess()
            persistence.fetchResult("43").expectSuccess()
            persistence.fetchResult("44").expectSuccess()
            persistence.fetchResult("45").expectSuccess()
            persistence.fetchResult("46") shouldBeEqual PersistenceAccessResult.uuidNotFound("46")
            persistence.fetchResult("47") shouldBeEqual PersistenceAccessResult.uuidNotFound("47")
            persistence.fetchResult("48").expectSuccess()
            persistence.fetchFailure("49") shouldBeEqual PersistenceAccessResult.uuidNotFound("49")
            persistence.fetchFailure("50").expectSuccess()
            persistence.fetchFailure("51") shouldBeEqual PersistenceAccessResult.uuidNotFound("51")
        }
    }

    test("test reset my running jobs") {
        val redis = RedisServer.newRedisServer().start()
        val persistence = newRedisPersistence<TestInput, TestResult>(redis)
        val testingMode = JobFrameworkTestingMode("I1", persistence, this, false) {
            addJob(defaultJobType, persistence, { _, _ -> ComputationResult.Success(TestResult(42)) }) {}
            addJob("other", persistence, { _, _ -> ComputationResult.Success(TestResult(42)) }) { jobConfig { maxRestarts = 2 } }
        }

        val directCall = suspend {
            Maintenance.resetMyRunningJobs(
                persistence,
                "I1",
                mapOf(defaultJobType to persistence, "other" to persistence),
                mapOf(defaultJobType to 3, "other" to 2)
            )
        }
        val testingCall = suspend { testingMode.resetMyRunningJobs() }

        for (method in listOf(directCall, testingCall)) {
            JedisPool(redis.host, redis.bindPort).resource.use { it.flushDB() }

            persistence.dataTransaction {
                persistJob(newJob("42", status = CREATED))
                persistJob(newJob("43", status = RUNNING, executingInstance = "I1"))
                persistJob(newJob("44", status = SUCCESS, executingInstance = "I1"))
                persistJob(newJob("45", status = FAILURE, executingInstance = "I1"))
                persistJob(newJob("46", status = CANCEL_REQUESTED, executingInstance = "I1"))
                persistJob(newJob("47", status = CANCELLED, executingInstance = "I1"))
                persistJob(newJob("48", status = RUNNING, executingInstance = "I2"))
                persistJob(newJob("49", status = RUNNING, executingInstance = "I1", numRestarts = 2))
                persistJob(newJob("50", status = RUNNING, executingInstance = "I3"))
                persistJob(newJob("51", status = RUNNING, executingInstance = "I1", numRestarts = 3))
                persistJob(newJob("52", jobType = "other", status = RUNNING, executingInstance = "I1", numRestarts = 1))
                persistJob(newJob("53", jobType = "other", status = RUNNING, executingInstance = "I1", numRestarts = 2))
            }
            method()
            persistence.fetchJob("42").expectSuccess().shouldHaveStatusAndRestarts(CREATED, 0)
            persistence.fetchJob("43").expectSuccess().shouldHaveStatusAndRestarts(CREATED, 1)
            persistence.fetchJob("44").expectSuccess().shouldHaveStatusAndRestarts(SUCCESS, 0)
            persistence.fetchJob("45").expectSuccess().shouldHaveStatusAndRestarts(FAILURE, 0)
            persistence.fetchJob("46").expectSuccess().shouldHaveStatusAndRestarts(CANCEL_REQUESTED, 0)
            persistence.fetchJob("47").expectSuccess().shouldHaveStatusAndRestarts(CANCELLED, 0)
            persistence.fetchJob("48").expectSuccess().shouldHaveStatusAndRestarts(RUNNING, 0)
            persistence.fetchJob("49").expectSuccess().shouldHaveStatusAndRestarts(CREATED, 3)
            persistence.fetchJob("50").expectSuccess().shouldHaveStatusAndRestarts(RUNNING, 0)
            persistence.fetchJob("51").expectSuccess().shouldHaveStatusAndRestarts(FAILURE, 3)
            persistence.fetchJob("52").expectSuccess().shouldHaveStatusAndRestarts(CREATED, 2)
            persistence.fetchJob("53").expectSuccess().shouldHaveStatusAndRestarts(FAILURE, 2)
            persistence.fetchFailure("51").expectSuccess() shouldBeEqual "The job was aborted because it exceeded the maximum number of 3 restarts"
            persistence.fetchFailure("53").expectSuccess() shouldBeEqual "The job was aborted because it exceeded the maximum number of 2 restarts"
        }
    }
})

private fun Job.shouldHaveStatusAndRestarts(status: JobStatus, numRestarts: Int) {
    this.status shouldBeEqual status
    this.numRestarts shouldBeEqual numRestarts
    if (status == CREATED) {
        executingInstance.shouldBeNull()
    }
}
