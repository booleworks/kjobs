// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.api.persistence

import com.booleworks.kjobs.api.persistence.hashmap.HashMapJobPersistence
import com.booleworks.kjobs.api.persistence.redis.RedisJobPersistence
import com.booleworks.kjobs.common.defaultJobType
import com.booleworks.kjobs.common.expectSuccess
import com.booleworks.kjobs.data.Heartbeat
import com.booleworks.kjobs.data.Job
import com.booleworks.kjobs.data.JobStatus
import com.booleworks.kjobs.data.PersistenceAccessResult
import com.booleworks.kjobs.data.uuidNotFound
import com.github.fppt.jedismock.RedisServer
import io.kotest.common.runBlocking
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.equals.shouldBeEqual
import redis.clients.jedis.JedisPool
import java.time.LocalDateTime

class JobPersistenceTest : FunSpec({

    fun testPersistences(testName: String, block: suspend (JobPersistence) -> Unit) = runBlocking {
        test("HashMap: $testName") { block(HashMapJobPersistence()) }
        val redis = RedisServer.newRedisServer().start()
        test("Redis: $testName") { block(RedisJobPersistence(JedisPool(redis.host, redis.bindPort))) }
//        redis.stop()
    }

    testPersistences("test persist and fetch") { persistence ->
        persistence.transaction {
            persistJob(newJob("42")).expectSuccess()
            persistJob(newJob("43")).expectSuccess()
            persistJob(newJob("44")).expectSuccess()
        }
        persistence.fetchJob("43").expectSuccess() shouldBeEqual newJob("43")
        persistence.fetchJob("44").expectSuccess() shouldBeEqual newJob("44")
        persistence.fetchJob("42").expectSuccess() shouldBeEqual newJob("42")
    }

    testPersistences("test job not found") { persistence ->
        persistence.fetchJob("42") shouldBeEqual PersistenceAccessResult.uuidNotFound("42")
        persistence.transaction { persistJob(newJob("42")).expectSuccess() }
        persistence.fetchJob("41") shouldBeEqual PersistenceAccessResult.uuidNotFound("41")
    }

    testPersistences("test update") { persistence ->
        persistence.transaction {
            persistJob(newJob("42")).expectSuccess()
            persistJob(newJob("43")).expectSuccess()
            persistJob(newJob("44")).expectSuccess()
            updateJob(
                newJob(
                    "43", status = JobStatus.RUNNING,
                    startedAt = LocalDateTime.of(2022, 7, 23, 0, 0), executingInstance = "You"
                )
            ).expectSuccess()
        }
        persistence.fetchJob("44").expectSuccess() shouldBeEqual newJob("44")
        persistence.fetchJob("42").expectSuccess() shouldBeEqual newJob("42")
        persistence.fetchJob("43").expectSuccess() shouldBeEqual newJob(
            "43", status = JobStatus.RUNNING,
            startedAt = LocalDateTime.of(2022, 7, 23, 0, 0), executingInstance = "You"
        )
    }
    
    testPersistences("test all jobs with status") { persistence ->
        persistence.transaction {
            persistJob(newJob("42", status = JobStatus.RUNNING)).expectSuccess()
            persistJob(newJob("43", status = JobStatus.SUCCESS)).expectSuccess()
            persistJob(newJob("44", status = JobStatus.CREATED)).expectSuccess()
            persistJob(newJob("45", status = JobStatus.FAILURE)).expectSuccess()
            persistJob(newJob("46", status = JobStatus.CANCELLED)).expectSuccess()
        }
        persistence.allJobsWithStatus(JobStatus.CANCEL_REQUESTED).expectSuccess() shouldHaveSize 0
        persistence.transaction {
            persistJob(newJob("47", status = JobStatus.CANCEL_REQUESTED)).expectSuccess()
            persistJob(newJob("48", status = JobStatus.RUNNING)).expectSuccess()
            persistJob(newJob("49", status = JobStatus.SUCCESS)).expectSuccess()
            persistJob(newJob("50", status = JobStatus.CREATED)).expectSuccess()
            persistJob(newJob("51", status = JobStatus.FAILURE)).expectSuccess()
            persistJob(newJob("52", status = JobStatus.CANCELLED)).expectSuccess()
            persistJob(newJob("53", status = JobStatus.CANCEL_REQUESTED)).expectSuccess()
        }
        persistence.allJobsWithStatus(JobStatus.SUCCESS).expectSuccess() shouldContainExactlyInAnyOrder listOf(
            newJob("43", status = JobStatus.SUCCESS),
            newJob("49", status = JobStatus.SUCCESS)
        )
        persistence.allJobsWithStatus(JobStatus.CANCELLED).expectSuccess() shouldContainExactlyInAnyOrder listOf(
            newJob("46", status = JobStatus.CANCELLED),
            newJob("52", status = JobStatus.CANCELLED)
        )
    }

    testPersistences("test all jobs of instance") { persistence ->
        persistence.transaction {
            persistJob(newJob("42", status = JobStatus.RUNNING, executingInstance = "I1")).expectSuccess()
            persistJob(newJob("43", status = JobStatus.SUCCESS, executingInstance = "I1")).expectSuccess()
            persistJob(newJob("44", status = JobStatus.CREATED, executingInstance = "I1")).expectSuccess()
            persistJob(newJob("45", status = JobStatus.FAILURE, executingInstance = "I1")).expectSuccess()
            persistJob(newJob("46", status = JobStatus.CANCELLED, executingInstance = "I1")).expectSuccess()
            persistJob(newJob("47", status = JobStatus.CANCEL_REQUESTED, executingInstance = "I1")).expectSuccess()
            persistJob(newJob("48", status = JobStatus.RUNNING, executingInstance = "I2")).expectSuccess()
            persistJob(newJob("49", status = JobStatus.SUCCESS, executingInstance = "I2")).expectSuccess()
            persistJob(newJob("50", status = JobStatus.CREATED, executingInstance = "I2")).expectSuccess()
            persistJob(newJob("51", status = JobStatus.FAILURE, executingInstance = "I2")).expectSuccess()
            persistJob(newJob("52", status = JobStatus.CANCELLED, executingInstance = "I2")).expectSuccess()
//            persistJob(newJob("53", status = JobStatus.CANCEL_REQUESTED, executingInstance = "I2")).expectSuccess()
            persistJob(newJob("54", status = JobStatus.RUNNING, executingInstance = "I1")).expectSuccess()
            persistJob(newJob("55", status = JobStatus.SUCCESS, executingInstance = "I1")).expectSuccess()
            persistJob(newJob("56", status = JobStatus.CREATED, executingInstance = "I1")).expectSuccess()
            persistJob(newJob("57", status = JobStatus.FAILURE, executingInstance = "I1")).expectSuccess()
            persistJob(newJob("58", status = JobStatus.CANCELLED, executingInstance = "I1")).expectSuccess()
            persistJob(newJob("59", status = JobStatus.CANCEL_REQUESTED, executingInstance = "I1")).expectSuccess()
        }
        persistence.allJobsOfInstance(JobStatus.SUCCESS, "I2").expectSuccess() shouldContainExactlyInAnyOrder listOf(
            newJob("49", status = JobStatus.SUCCESS, executingInstance = "I2")
        )
        persistence.allJobsOfInstance(JobStatus.CANCELLED, "I1").expectSuccess() shouldContainExactlyInAnyOrder listOf(
            newJob("46", status = JobStatus.CANCELLED, executingInstance = "I1"),
            newJob("58", status = JobStatus.CANCELLED, executingInstance = "I1")
        )
        persistence.allJobsOfInstance(JobStatus.CANCEL_REQUESTED, "I2").expectSuccess() shouldHaveSize 0
        persistence.allJobsOfInstance(JobStatus.CREATED, "I5").expectSuccess() shouldHaveSize 0
    }

    testPersistences("test all jobs finished before") { persistence ->
        val now = LocalDateTime.now()
        persistence.allJobsFinishedBefore(now).expectSuccess() shouldHaveSize 0
        persistence.transaction {
            persistJob(newJob("42", status = JobStatus.SUCCESS, finishedAt = now)).expectSuccess()
            persistJob(newJob("43", status = JobStatus.SUCCESS, finishedAt = now.minusSeconds(1))).expectSuccess()
            persistJob(newJob("44", status = JobStatus.SUCCESS, finishedAt = now.plusHours(2))).expectSuccess()
            persistJob(newJob("45", status = JobStatus.FAILURE, finishedAt = now)).expectSuccess()
            persistJob(newJob("46", status = JobStatus.FAILURE, finishedAt = now.plusDays(100))).expectSuccess()
            persistJob(newJob("47", status = JobStatus.FAILURE, finishedAt = now.minusDays(10))).expectSuccess()
            persistJob(newJob("48", status = JobStatus.CANCELLED, finishedAt = now.minusSeconds(1))).expectSuccess()
            persistJob(newJob("49", status = JobStatus.CANCEL_REQUESTED, finishedAt = now.minusSeconds(1))).expectSuccess()
            persistJob(newJob("50", status = JobStatus.CREATED, finishedAt = now.minusSeconds(1))).expectSuccess()
            persistJob(newJob("51", status = JobStatus.RUNNING, finishedAt = now.minusSeconds(1))).expectSuccess()
        }

        persistence.allJobsFinishedBefore(now).expectSuccess() shouldContainExactlyInAnyOrder listOf(
            newJob("43", status = JobStatus.SUCCESS, finishedAt = now.minusSeconds(1)),
            newJob("47", status = JobStatus.FAILURE, finishedAt = now.minusDays(10)),
        )
    }

    testPersistences("test fetch states") { persistence ->
        val now = LocalDateTime.now()
        persistence.fetchStates(listOf("42", "43")) shouldBeEqual PersistenceAccessResult.uuidNotFound("42")
        persistence.transaction {
            persistJob(newJob("42", status = JobStatus.SUCCESS, finishedAt = now)).expectSuccess()
            persistJob(newJob("43", status = JobStatus.SUCCESS, finishedAt = now.minusSeconds(1))).expectSuccess()
            persistJob(newJob("44", status = JobStatus.SUCCESS, finishedAt = now.plusHours(2))).expectSuccess()
            persistJob(newJob("45", status = JobStatus.FAILURE, finishedAt = now)).expectSuccess()
            persistJob(newJob("46", status = JobStatus.FAILURE, finishedAt = now.plusDays(100))).expectSuccess()
            persistJob(newJob("47", status = JobStatus.FAILURE, finishedAt = now.minusDays(10))).expectSuccess()
            persistJob(newJob("48", status = JobStatus.CANCELLED, finishedAt = now.minusSeconds(1))).expectSuccess()
            persistJob(newJob("49", status = JobStatus.CANCEL_REQUESTED, finishedAt = now.minusSeconds(1))).expectSuccess()
            persistJob(newJob("50", status = JobStatus.CREATED, finishedAt = now.minusSeconds(1))).expectSuccess()
            persistJob(newJob("51", status = JobStatus.RUNNING, finishedAt = now.minusSeconds(1))).expectSuccess()
        }
        persistence.fetchStates(listOf("42", "41", "43", "55")) shouldBeEqual PersistenceAccessResult.uuidNotFound("41")
        persistence.fetchStates(listOf("42", "43", "45", "44", "46", "47", "48", "50", "49", "51")).expectSuccess() shouldBeEqual
                listOf(
                    JobStatus.SUCCESS, JobStatus.SUCCESS, JobStatus.FAILURE, JobStatus.SUCCESS, JobStatus.FAILURE,
                    JobStatus.FAILURE, JobStatus.CANCELLED, JobStatus.CREATED, JobStatus.CANCEL_REQUESTED, JobStatus.RUNNING,
                )
    }

    test("HashMap: test heartbeats") {
        val persistence = HashMapJobPersistence()
        val now = LocalDateTime.now()
        persistence.fetchHeartbeats(now).expectSuccess() shouldHaveSize 0
        persistence.fetchHeartbeats(now.minusSeconds(5)).expectSuccess() shouldHaveSize 1

        persistence.updateHeartbeat(Heartbeat("HUGO", now)).expectSuccess()
        persistence.updateHeartbeat(Heartbeat("ME", now)).expectSuccess()
        persistence.fetchHeartbeats(now).expectSuccess() shouldContainExactly listOf(Heartbeat("ME", now))
        persistence.fetchHeartbeats(now.minusSeconds(1)).expectSuccess() shouldContainExactly listOf(Heartbeat("ME", now))
        persistence.fetchHeartbeats(now.plusSeconds(1)).expectSuccess() shouldHaveSize 0
    }

    test("Redis: test heartbeats") {
        val redis = RedisServer.newRedisServer().start()
        val persistence = RedisJobPersistence(JedisPool(redis.host, redis.bindPort))
        val now = LocalDateTime.now()
        persistence.fetchHeartbeats(now.minusYears(2000)).expectSuccess() shouldHaveSize 0
        persistence.transaction {
            updateHeartbeat(Heartbeat("I1", now.minusSeconds(3)))
            updateHeartbeat(Heartbeat("I1", now))
            updateHeartbeat(Heartbeat("I2", now.plusMinutes(1)))
            updateHeartbeat(Heartbeat("I2", now.minusSeconds(1)))
            updateHeartbeat(Heartbeat("I3", now.plusSeconds(2)))
            updateHeartbeat(Heartbeat("I4", now.minusDays(5)))
            updateHeartbeat(Heartbeat("I4", now.plusDays(5)))
        }
        persistence.fetchHeartbeats(now).expectSuccess() shouldContainExactlyInAnyOrder listOf(
            Heartbeat("I1", now),
            Heartbeat("I3", now.plusSeconds(2)),
            Heartbeat("I4", now.plusDays(5)),
        )
    }
})

internal fun newJob(
    uuid: String,
    jobType: String = defaultJobType,
    status: JobStatus = JobStatus.CREATED,
    createdAt: LocalDateTime = LocalDateTime.of(2022, 7, 22, 0, 0),
    startedAt: LocalDateTime? = null,
    executingInstance: String? = null,
    finishedAt: LocalDateTime? = null,
    timeout: LocalDateTime? = null,
    numRestarts: Int = 0
) =
    Job(
        uuid, jobType, listOf("T1", "T2"), "Test $uuid", 5, "ME", createdAt, status, startedAt, executingInstance, finishedAt, timeout, numRestarts
    )
