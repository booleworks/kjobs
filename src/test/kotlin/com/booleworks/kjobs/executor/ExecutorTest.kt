// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.executor

import com.booleworks.kjobs.api.JobFrameworkTestingMode
import com.booleworks.kjobs.api.persistence.hashmap.HashMapDataPersistence
import com.booleworks.kjobs.api.persistence.hashmap.HashMapJobPersistence
import com.booleworks.kjobs.common.Either
import com.booleworks.kjobs.common.TestInput
import com.booleworks.kjobs.common.TestResult
import com.booleworks.kjobs.common.defaultExecutor
import com.booleworks.kjobs.common.defaultInstanceName
import com.booleworks.kjobs.common.defaultJobType
import com.booleworks.kjobs.common.right
import com.booleworks.kjobs.common.testWithRedis
import com.booleworks.kjobs.control.ComputationResult
import com.booleworks.kjobs.data.Job
import com.booleworks.kjobs.data.JobStatus
import com.booleworks.kjobs.data.orQuitWith
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.fail
import org.junit.jupiter.api.Test
import java.time.LocalDateTime.now
import java.util.*
import kotlin.time.Duration.Companion.milliseconds

class ExecutorTest {
    
    @Test
    fun `test with testing mode`(): Unit = runBlocking {
        val jobPersistence = HashMapJobPersistence()
        val dataPersistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
        val testingMode = JobFrameworkTestingMode("ME", jobPersistence, Either.Left(this@runBlocking), false) {
            addJob("TestJob", dataPersistence, { _, input: TestInput -> ComputationResult.Success(TestResult(input.value)) }) {}
        }
        val job = testingMode.submitJob("TestJob", TestInput(42)).orQuitWith { fail("Expected persistence access to succeed") }
        assertThat(job.type).isEqualTo("TestJob")
        assertThat(job.tags).isEmpty()
        assertThat(job.customInfo).isNull()
        assertThat(job.priority).isZero()
        assertThat(job.createdBy).isEqualTo("ME")
        assertThat(job.createdAt).isBetween(now().minusSeconds(10), now())
        assertThat(job.status).isEqualTo(JobStatus.CREATED)
        assertThat(job.startedAt).isNull()
        assertThat(job.executingInstance).isNull()
        assertThat(job.finishedAt).isNull()
        assertThat(job.timeout).isNull()
        assertThat(job.numRestarts).isZero()
        testingMode.runExecutor()
        val jobAfterComputation = jobPersistence.fetchJob(job.uuid).orQuitWith { fail("Expected persistence access to succeed") }
        assertThat(jobAfterComputation.uuid).isEqualTo(job.uuid)
        assertThat(jobAfterComputation.type).isEqualTo("TestJob")
        assertThat(jobAfterComputation.tags).isEmpty()
        assertThat(jobAfterComputation.customInfo).isNull()
        assertThat(jobAfterComputation.priority).isZero()
        assertThat(jobAfterComputation.createdBy).isEqualTo("ME")
        assertThat(jobAfterComputation.createdAt).isEqualTo(job.createdAt)
        assertThat(jobAfterComputation.status).isEqualTo(JobStatus.SUCCESS)
        assertThat(jobAfterComputation.startedAt).isBetween(now().minusSeconds(10), now()).isAfter(jobAfterComputation.createdAt)
        assertThat(jobAfterComputation.executingInstance).isEqualTo("ME")
        assertThat(jobAfterComputation.finishedAt).isBetween(now().minusSeconds(10), now()).isAfter(jobAfterComputation.startedAt)
        assertThat(jobAfterComputation.timeout).isAfter(jobAfterComputation.finishedAt)
        assertThat(jobAfterComputation.numRestarts).isZero()
        assertThat(dataPersistence.fetchResult(job.uuid).orQuitWith { fail("Expected persistence access to succeed") }).isEqualTo(TestResult(42))
    }

    @Test
    fun `test simple computation with redis`() = testWithRedis {
        val job = newJob()
        val input = TestInput(42)
        dataTransaction { persistJob(job); persistInput(job, input) }
        defaultExecutor(this).execute()
        val jobAfterComputation = fetchJob(job.uuid).right()
        assertThat(jobAfterComputation.uuid).isEqualTo(job.uuid)
        assertThat(jobAfterComputation.type).isEqualTo("TestJob")
        assertThat(jobAfterComputation.tags).isEmpty()
        assertThat(jobAfterComputation.customInfo).isNull()
        assertThat(jobAfterComputation.priority).isZero()
        assertThat(jobAfterComputation.createdBy).isEqualTo(defaultInstanceName)
        assertThat(jobAfterComputation.createdAt).isEqualTo(job.createdAt)
        assertThat(jobAfterComputation.status).isEqualTo(JobStatus.SUCCESS)
        assertThat(jobAfterComputation.startedAt).isBetween(job.createdAt, now())
        assertThat(jobAfterComputation.executingInstance).isEqualTo(defaultInstanceName)
        assertThat(jobAfterComputation.finishedAt).isBetween(jobAfterComputation.startedAt, now())
        assertThat(jobAfterComputation.timeout).isAfter(now())
        assertThat(jobAfterComputation.numRestarts).isZero()
        assertThat(fetchResult(job.uuid).right()).isEqualTo(TestResult(42))
    }

    @Test
    fun `test computation with exception with redis`() = testWithRedis {
        val job = newJob()
        val input = TestInput(42, throwException = true)
        dataTransaction { persistJob(job); persistInput(job, input) }
        defaultExecutor(this).execute()
        val jobAfterComputation = fetchJob(job.uuid).right()
        assertThat(jobAfterComputation.uuid).isEqualTo(job.uuid)
        assertThat(jobAfterComputation.type).isEqualTo("TestJob")
        assertThat(jobAfterComputation.tags).isEmpty()
        assertThat(jobAfterComputation.customInfo).isNull()
        assertThat(jobAfterComputation.priority).isZero()
        assertThat(jobAfterComputation.createdBy).isEqualTo(defaultInstanceName)
        assertThat(jobAfterComputation.createdAt).isEqualTo(job.createdAt)
        assertThat(jobAfterComputation.status).isEqualTo(JobStatus.FAILURE)
        assertThat(jobAfterComputation.startedAt).isBetween(job.createdAt, now())
        assertThat(jobAfterComputation.executingInstance).isEqualTo(defaultInstanceName)
        assertThat(jobAfterComputation.finishedAt).isBetween(jobAfterComputation.startedAt, now())
        assertThat(jobAfterComputation.timeout).isAfter(now())
        assertThat(jobAfterComputation.numRestarts).isZero()
        assertThat(fetchFailure(job.uuid).right()).isEqualTo("Unexpected exception during computation: Test Exception Message")
    }

    @Test
    fun `test computation with timeout with redis`() = testWithRedis {
        val job = newJob()
        val input = TestInput(42, expectedDelay = 10, throwException = true)
        dataTransaction { persistJob(job); persistInput(job, input) }
        defaultExecutor(this, timeout = { _, _ -> 1.milliseconds }).execute()
        val jobAfterComputation = fetchJob(job.uuid).right()
        assertThat(jobAfterComputation.uuid).isEqualTo(job.uuid)
        assertThat(jobAfterComputation.type).isEqualTo("TestJob")
        assertThat(jobAfterComputation.tags).isEmpty()
        assertThat(jobAfterComputation.customInfo).isNull()
        assertThat(jobAfterComputation.priority).isZero()
        assertThat(jobAfterComputation.createdBy).isEqualTo(defaultInstanceName)
        assertThat(jobAfterComputation.createdAt).isEqualTo(job.createdAt)
        assertThat(jobAfterComputation.status).isEqualTo(JobStatus.FAILURE)
        assertThat(jobAfterComputation.startedAt).isBetween(job.createdAt, now())
        assertThat(jobAfterComputation.executingInstance).isEqualTo(defaultInstanceName)
        assertThat(jobAfterComputation.finishedAt).isBetween(jobAfterComputation.startedAt, now())
        assertThat(jobAfterComputation.timeout).isBefore(now())
        assertThat(jobAfterComputation.numRestarts).isZero()
        assertThat(fetchFailure(job.uuid).right()).isEqualTo("The job did not finish within the configured timeout of 1ms")
    }

    private fun newJob() = Job(UUID.randomUUID().toString(), defaultJobType, emptyList(), null, 0, defaultInstanceName, now(), JobStatus.CREATED)
}
