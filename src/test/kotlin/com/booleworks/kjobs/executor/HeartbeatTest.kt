// SPDX-License-Identifier: MIT
// Copyright 2023-2024 BooleWorks GmbH

package com.booleworks.kjobs.executor

import com.booleworks.kjobs.common.defaultInstanceName
import com.booleworks.kjobs.common.defaultJobType
import com.booleworks.kjobs.common.right
import com.booleworks.kjobs.common.testWithRedis
import com.booleworks.kjobs.control.Maintenance
import com.booleworks.kjobs.control.scheduleForever
import com.booleworks.kjobs.data.Job
import com.booleworks.kjobs.data.JobStatus
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.equals.shouldBeEqual
import io.kotest.matchers.ints.shouldBeZero
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import java.time.LocalDateTime.now
import java.util.UUID
import kotlin.time.Duration.Companion.milliseconds

class HeartbeatTest : FunSpec({

    testWithRedis("test heartbeat keeps job alive") {
        val job = newJob().apply {
            status = JobStatus.RUNNING
            executingInstance = defaultInstanceName
        }
        transaction { persistJob(job) }
        coroutineScope {
            coroutineContext.scheduleForever(20.milliseconds, "test") { Maintenance.updateHeartbeat(this@testWithRedis, defaultInstanceName) }
            coroutineContext.scheduleForever(20.milliseconds, "test") {
                Maintenance.restartJobsFromDeadInstances(
                    this@testWithRedis,
                    mapOf(defaultJobType to this@testWithRedis),
                    20.milliseconds,
                    mapOf(defaultJobType to 3)
                )
            }
            delay(100.milliseconds)
            with(fetchJob(job.uuid).right()) {
                status shouldBeEqual (JobStatus.RUNNING)
                numRestarts.shouldBeZero()
                executingInstance!! shouldBeEqual defaultInstanceName
            }
            coroutineContext.cancelChildren()
        }
    }

    testWithRedis("test dead instance") {
        val job = newJob().apply {
            status = JobStatus.RUNNING
            executingInstance = defaultInstanceName
            startedAt = now()
            timeout = now().plusSeconds(10)
            numRestarts = 2
        }
        transaction { persistJob(job) }
        coroutineScope {
            val heartbeat = coroutineContext.scheduleForever(20.milliseconds, "test") { Maintenance.updateHeartbeat(this@testWithRedis, defaultInstanceName) }
            coroutineContext.scheduleForever(20.milliseconds, "test") {
                Maintenance.restartJobsFromDeadInstances(
                    this@testWithRedis,
                    mapOf(defaultJobType to this@testWithRedis),
                    20.milliseconds,
                    mapOf(defaultJobType to 3)
                )
            }
            delay(100.milliseconds)
            with(fetchJob(job.uuid).right()) {
                status shouldBeEqual JobStatus.RUNNING
                numRestarts shouldBeEqual 2
                executingInstance!! shouldBeEqual defaultInstanceName
                startedAt.shouldNotBeNull()
                timeout.shouldNotBeNull()
            }
            heartbeat.cancelAndJoin()
            delay(100.milliseconds)
            with(fetchJob(job.uuid).right()) {
                status shouldBeEqual JobStatus.CREATED
                numRestarts shouldBeEqual 3
                executingInstance.shouldBeNull()
                startedAt.shouldBeNull()
                timeout.shouldBeNull()
            }
            coroutineContext.cancelChildren()
        }
    }

    testWithRedis("test dead instance and max restarts reached") {
        val job = newJob().apply {
            status = JobStatus.RUNNING
            executingInstance = defaultInstanceName
            startedAt = now()
            timeout = now().plusSeconds(10)
            numRestarts = 3
        }
        transaction { persistJob(job) }
        coroutineScope {
            val heartbeat = coroutineContext.scheduleForever(20.milliseconds, "test") { Maintenance.updateHeartbeat(this@testWithRedis, defaultInstanceName) }
            coroutineContext.scheduleForever(20.milliseconds, "test") {
                Maintenance.restartJobsFromDeadInstances(
                    this@testWithRedis,
                    mapOf(defaultJobType to this@testWithRedis),
                    20.milliseconds,
                    mapOf(defaultJobType to 3)
                )
            }
            delay(100.milliseconds)
            with(fetchJob(job.uuid).right()) {
                status shouldBeEqual JobStatus.RUNNING
                numRestarts shouldBeEqual 3
                executingInstance!! shouldBeEqual defaultInstanceName
                startedAt.shouldNotBeNull()
                timeout.shouldNotBeNull()
            }
            heartbeat.cancelAndJoin()
            delay(100.milliseconds)
            with(fetchJob(job.uuid).right()) {
                status shouldBeEqual JobStatus.FAILURE
                numRestarts shouldBeEqual 3
                executingInstance!! shouldBeEqual defaultInstanceName
                startedAt.shouldNotBeNull()
                timeout.shouldNotBeNull()
            }
            fetchFailure(job.uuid).right() shouldBeEqual "The job was aborted because it exceeded the maximum number of 3 restarts"
            coroutineContext.cancelChildren()
        }
    }

    testWithRedis("test with live and dead jobs") {
        val deadJob = newJob().apply {
            status = JobStatus.RUNNING
            executingInstance = "Dead instance"
            startedAt = now()
            timeout = now().plusSeconds(10)
        }
        val liveJob = newJob().apply {
            status = JobStatus.RUNNING
            executingInstance = "Live instance"
            startedAt = now()
            timeout = now().plusSeconds(10)
        }
        transaction { persistJob(deadJob); persistJob(liveJob) }
        coroutineScope {
            coroutineContext.scheduleForever(20.milliseconds, "test") { Maintenance.updateHeartbeat(this@testWithRedis, "Live instance") }
            coroutineContext.scheduleForever(20.milliseconds, "test") { Maintenance.updateHeartbeat(this@testWithRedis, "Other instance") }
            coroutineContext.scheduleForever(20.milliseconds, "test") {
                Maintenance.restartJobsFromDeadInstances(
                    this@testWithRedis,
                    mapOf(defaultJobType to this@testWithRedis),
                    20.milliseconds,
                    mapOf(defaultJobType to 3)
                )
            }
            delay(100.milliseconds)
            with(fetchJob(deadJob.uuid).right()) {
                status shouldBeEqual JobStatus.CREATED
                numRestarts shouldBeEqual 1
                executingInstance.shouldBeNull()
                startedAt.shouldBeNull()
                timeout.shouldBeNull()
            }
            with(fetchJob(liveJob.uuid).right()) {
                status shouldBeEqual JobStatus.RUNNING
                numRestarts.shouldBeZero()
                executingInstance!! shouldBeEqual "Live instance"
                startedAt.shouldNotBeNull()
                timeout.shouldNotBeNull()
            }
            coroutineContext.cancelChildren()
        }
    }
})

private fun newJob() = Job(UUID.randomUUID().toString(), defaultJobType, emptyList(), null, 0, defaultInstanceName, now(), JobStatus.CREATED)
