// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.executor

import com.booleworks.kjobs.data.Job
import com.booleworks.kjobs.data.JobResult
import com.booleworks.kjobs.data.JobStatus
import com.booleworks.kjobs.util.TestInput
import com.booleworks.kjobs.util.TestResult
import com.booleworks.kjobs.util.defaultExecutor
import com.booleworks.kjobs.util.defaultInstanceName
import com.booleworks.kjobs.util.defaultJobType
import com.booleworks.kjobs.util.right
import com.booleworks.kjobs.util.testWithRedis
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.LocalDateTime.now
import java.util.*
import kotlin.time.Duration.Companion.milliseconds

class ExecutorTest {

    @Test
    fun `test simple computation`() = testWithRedis {
        val job = newJob()
        val input = TestInput(42)
        dataTransaction { persistJob(job); persistInput(job, input) }
        defaultExecutor(this).execute()
        val jobAfterComputation = fetchJob(job.uuid).right()
        assertThat(jobAfterComputation.uuid).isEqualTo(job.uuid)
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
        assertThat(fetchResult(job.uuid).right()).isEqualTo(JobResult.success(job.uuid, TestResult(42)))
    }

    @Test
    fun `test computation with exception`() = testWithRedis {
        val job = newJob()
        val input = TestInput(42, throwException = true)
        dataTransaction { persistJob(job); persistInput(job, input) }
        defaultExecutor(this).execute()
        val jobAfterComputation = fetchJob(job.uuid).right()
        assertThat(jobAfterComputation.uuid).isEqualTo(job.uuid)
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
        assertThat(fetchResult(job.uuid).right()).isEqualTo(
            JobResult.error<Any>(job.uuid, "Unexpected exception during computation: Test Exception Message")
        )
    }

    @Test
    fun `test computation with timeout`() = testWithRedis {
        val job = newJob()
        val input = TestInput(42, expectedDelay = 10, throwException = true)
        dataTransaction { persistJob(job); persistInput(job, input) }
        defaultExecutor(this, timeout = { _, _ -> 1.milliseconds }).execute()
        val jobAfterComputation = fetchJob(job.uuid).right()
        assertThat(jobAfterComputation.uuid).isEqualTo(job.uuid)
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
        assertThat(fetchResult(job.uuid).right()).isEqualTo(
            JobResult.error<Any>(job.uuid, "The job did not finish within the configured timeout of 1ms")
        )
    }

    private fun newJob() = Job(UUID.randomUUID().toString(), defaultJobType, emptyList(), null, 0, defaultInstanceName, now(), JobStatus.CREATED)
}
