// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.jobframework.executor

import com.booleworks.jobframework.control.Maintenance
import com.booleworks.jobframework.control.scheduleForever
import com.booleworks.jobframework.data.Job
import com.booleworks.jobframework.data.JobStatus
import com.booleworks.jobframework.util.defaultInstanceName
import com.booleworks.jobframework.util.defaultJobType
import com.booleworks.jobframework.util.right
import com.booleworks.jobframework.util.testWithRedis
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.LocalDateTime.now
import java.util.*
import kotlin.time.Duration.Companion.milliseconds

class HeartbeatTest {

    @Test
    fun `test heartbeat keeps job alive`() = testWithRedis {
        val job = newJob().apply {
            status = JobStatus.RUNNING
            executingInstance = defaultInstanceName
        }
        transaction { persistJob(job) }
        coroutineScope {
            scheduleForever(5.milliseconds) { Maintenance.updateHeartbeat(this@testWithRedis, defaultInstanceName) }
            scheduleForever(5.milliseconds) {
                Maintenance.restartJobsFromDeadInstances(
                    this@testWithRedis,
                    mapOf(defaultJobType to this@testWithRedis),
                    5.milliseconds,
                    3
                )
            }
            delay(100.milliseconds)
            with(fetchJob(job.uuid).right()) {
                assertThat(status).isEqualTo(JobStatus.RUNNING)
                assertThat(numRestarts).isZero()
                assertThat(executingInstance).isEqualTo(defaultInstanceName)
            }
            coroutineContext.cancelChildren()
        }
    }

    @Test
    fun `test dead instance`() = testWithRedis {
        val job = newJob().apply {
            status = JobStatus.RUNNING
            executingInstance = defaultInstanceName
            startedAt = now()
            timeout = now().plusSeconds(10)
            numRestarts = 2
        }
        transaction { persistJob(job) }
        coroutineScope {
            val heartbeat = scheduleForever(5.milliseconds) { Maintenance.updateHeartbeat(this@testWithRedis, defaultInstanceName) }
            scheduleForever(5.milliseconds) {
                Maintenance.restartJobsFromDeadInstances(
                    this@testWithRedis,
                    mapOf(defaultJobType to this@testWithRedis),
                    5.milliseconds,
                    3
                )
            }
            delay(20.milliseconds)
            with(fetchJob(job.uuid).right()) {
                assertThat(status).isEqualTo(JobStatus.RUNNING)
                assertThat(numRestarts).isEqualTo(2)
                assertThat(executingInstance).isEqualTo(defaultInstanceName)
                assertThat(startedAt).isNotNull()
                assertThat(timeout).isNotNull()
            }
            heartbeat.cancelAndJoin()
            delay(20.milliseconds)
            with(fetchJob(job.uuid).right()) {
                assertThat(status).isEqualTo(JobStatus.CREATED)
                assertThat(numRestarts).isEqualTo(3)
                assertThat(executingInstance).isNull()
                assertThat(startedAt).isNull()
                assertThat(timeout).isNull()
            }
            coroutineContext.cancelChildren()
        }
    }

    @Test
    fun `test dead instance and max restarts reached`() = testWithRedis {
        val job = newJob().apply {
            status = JobStatus.RUNNING
            executingInstance = defaultInstanceName
            startedAt = now()
            timeout = now().plusSeconds(10)
            numRestarts = 3
        }
        transaction { persistJob(job) }
        coroutineScope {
            val heartbeat = scheduleForever(5.milliseconds) { Maintenance.updateHeartbeat(this@testWithRedis, defaultInstanceName) }
            scheduleForever(5.milliseconds) {
                Maintenance.restartJobsFromDeadInstances(
                    this@testWithRedis,
                    mapOf(defaultJobType to this@testWithRedis),
                    5.milliseconds,
                    3
                )
            }
            delay(20.milliseconds)
            with(fetchJob(job.uuid).right()) {
                assertThat(status).isEqualTo(JobStatus.RUNNING)
                assertThat(numRestarts).isEqualTo(3)
                assertThat(executingInstance).isEqualTo(defaultInstanceName)
                assertThat(startedAt).isNotNull()
                assertThat(timeout).isNotNull()
            }
            heartbeat.cancelAndJoin()
            delay(20.milliseconds)
            with(fetchJob(job.uuid).right()) {
                assertThat(status).isEqualTo(JobStatus.FAILURE)
                assertThat(numRestarts).isEqualTo(3)
                assertThat(executingInstance).isEqualTo(defaultInstanceName)
                assertThat(startedAt).isNotNull()
                assertThat(timeout).isNotNull()
            }
            assertThat(fetchResult(job.uuid).right().error).isEqualTo("The job was aborted because it exceeded the number of 3 restarts")
            coroutineContext.cancelChildren()
        }
    }

    @Test
    fun `test with live and dead jobs`() = testWithRedis {
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
            scheduleForever(5.milliseconds) { Maintenance.updateHeartbeat(this@testWithRedis, "Live instance") }
            scheduleForever(5.milliseconds) { Maintenance.updateHeartbeat(this@testWithRedis, "Other instance") }
            scheduleForever(5.milliseconds) {
                Maintenance.restartJobsFromDeadInstances(
                    this@testWithRedis,
                    mapOf(defaultJobType to this@testWithRedis),
                    5.milliseconds,
                    3
                )
            }
            delay(20.milliseconds)
            with(fetchJob(deadJob.uuid).right()) {
                assertThat(status).isEqualTo(JobStatus.CREATED)
                assertThat(numRestarts).isOne()
                assertThat(executingInstance).isNull()
                assertThat(startedAt).isNull()
                assertThat(timeout).isNull()
            }
            with(fetchJob(liveJob.uuid).right()) {
                assertThat(status).isEqualTo(JobStatus.RUNNING)
                assertThat(numRestarts).isZero()
                assertThat(executingInstance).isEqualTo("Live instance")
                assertThat(startedAt).isNotNull()
                assertThat(timeout).isNotNull()
            }
            coroutineContext.cancelChildren()
        }
    }

    private fun newJob() = Job(UUID.randomUUID().toString(), defaultJobType, emptyList(), null, 0, defaultInstanceName, now(), JobStatus.CREATED)
}
