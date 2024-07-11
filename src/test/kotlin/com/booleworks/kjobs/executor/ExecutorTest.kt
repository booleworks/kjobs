// SPDX-License-Identifier: MIT
// Copyright 2023-2024 BooleWorks GmbH

package com.booleworks.kjobs.executor

import com.booleworks.kjobs.api.JobFrameworkTestingMode
import com.booleworks.kjobs.api.persistence.hashmap.HashMapDataPersistence
import com.booleworks.kjobs.api.persistence.hashmap.HashMapJobPersistence
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
import io.kotest.assertions.fail
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.date.shouldBeAfter
import io.kotest.matchers.date.shouldBeBefore
import io.kotest.matchers.date.shouldBeBetween
import io.kotest.matchers.equals.shouldBeEqual
import io.kotest.matchers.ints.shouldBeZero
import io.kotest.matchers.nulls.shouldBeNull
import java.time.LocalDateTime.now
import java.util.UUID
import kotlin.time.Duration.Companion.milliseconds

class ExecutorTest : FunSpec({

    test("test with testing mode") {
        val jobPersistence = HashMapJobPersistence()
        val dataPersistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
        val testingMode = JobFrameworkTestingMode("ME", jobPersistence, false) {
            addJob("TestJob", dataPersistence, { _, input: TestInput -> ComputationResult.Success(TestResult(input.value)) })
        }
        val job = testingMode.submitJob("TestJob", TestInput(42)).orQuitWith { fail("Expected persistence access to succeed") }
        job.type shouldBeEqual "TestJob"
        job.tags.isEmpty()
        job.customInfo.shouldBeNull()
        job.priority.shouldBeZero()
        job.createdBy shouldBeEqual "ME"
        job.createdAt.shouldBeBetween(now().minusSeconds(10), now())
        job.status shouldBeEqual JobStatus.CREATED
        job.startedAt.shouldBeNull()
        job.executingInstance.shouldBeNull()
        job.finishedAt.shouldBeNull()
        job.timeout.shouldBeNull()
        job.numRestarts.shouldBeZero()
        testingMode.runExecutor()
        val jobAfterComputation = jobPersistence.fetchJob(job.uuid).orQuitWith { fail("Expected persistence access to succeed") }
        jobAfterComputation.uuid shouldBeEqual job.uuid
        jobAfterComputation.type shouldBeEqual "TestJob"
        jobAfterComputation.tags.isEmpty()
        jobAfterComputation.customInfo.shouldBeNull()
        jobAfterComputation.priority.shouldBeZero()
        jobAfterComputation.createdBy shouldBeEqual "ME"
        jobAfterComputation.createdAt shouldBeEqual job.createdAt
        jobAfterComputation.status shouldBeEqual JobStatus.SUCCESS
        jobAfterComputation.startedAt!!.shouldBeBetween(now().minusSeconds(10), now())
        jobAfterComputation.startedAt!! shouldBeAfter jobAfterComputation.createdAt
        jobAfterComputation.executingInstance!! shouldBeEqual "ME"
        jobAfterComputation.finishedAt!!.shouldBeBetween(now().minusSeconds(10), now())
        jobAfterComputation.finishedAt!! shouldBeAfter jobAfterComputation.startedAt!!
        jobAfterComputation.timeout!! shouldBeAfter jobAfterComputation.finishedAt!!
        jobAfterComputation.numRestarts.shouldBeZero()
        dataPersistence.fetchResult(job.uuid).orQuitWith { fail("Expected persistence access to succeed") } shouldBeEqual TestResult(42)
    }

    testWithRedis("test simple computation with redis") {
        val job = newJob()
        val input = TestInput(42)
        dataTransaction { persistJob(job); persistInput(job, input) }
        defaultExecutor(this).execute()
        val jobAfterComputation = fetchJob(job.uuid).right()
        jobAfterComputation.uuid shouldBeEqual job.uuid
        jobAfterComputation.type shouldBeEqual "TestJob"
        jobAfterComputation.tags.isEmpty()
        jobAfterComputation.customInfo.shouldBeNull()
        jobAfterComputation.priority.shouldBeZero()
        jobAfterComputation.createdBy shouldBeEqual defaultInstanceName
        jobAfterComputation.createdAt shouldBeEqual job.createdAt
        jobAfterComputation.status shouldBeEqual JobStatus.SUCCESS
        jobAfterComputation.startedAt!!.shouldBeBetween(job.createdAt, now())
        jobAfterComputation.executingInstance!! shouldBeEqual defaultInstanceName
        jobAfterComputation.finishedAt!!.shouldBeBetween(jobAfterComputation.startedAt!!, now())
        jobAfterComputation.timeout!! shouldBeAfter now()
        jobAfterComputation.numRestarts.shouldBeZero()
        fetchResult(job.uuid).right() shouldBeEqual TestResult(42)
    }

    testWithRedis("test computation with exception with redis") {
        val job = newJob()
        val input = TestInput(42, throwException = true)
        dataTransaction { persistJob(job); persistInput(job, input) }
        defaultExecutor(this).execute()
        val jobAfterComputation = fetchJob(job.uuid).right()
        jobAfterComputation.uuid shouldBeEqual job.uuid
        jobAfterComputation.type shouldBeEqual "TestJob"
        jobAfterComputation.tags.isEmpty()
        jobAfterComputation.customInfo.shouldBeNull()
        jobAfterComputation.priority.shouldBeZero()
        jobAfterComputation.createdBy shouldBeEqual defaultInstanceName
        jobAfterComputation.createdAt shouldBeEqual job.createdAt
        jobAfterComputation.status shouldBeEqual JobStatus.FAILURE
        jobAfterComputation.startedAt!!.shouldBeBetween(job.createdAt, now())
        jobAfterComputation.executingInstance!! shouldBeEqual defaultInstanceName
        jobAfterComputation.finishedAt!!.shouldBeBetween(jobAfterComputation.startedAt!!, now())
        jobAfterComputation.timeout!! shouldBeAfter now()
        jobAfterComputation.numRestarts.shouldBeZero()
        fetchFailure(job.uuid).right() shouldBeEqual "Unexpected exception during computation: Test Exception Message"
    }

    testWithRedis("test computation with timeout with redis") {
        val job = newJob()
        val input = TestInput(42, expectedDelay = 10, throwException = true)
        dataTransaction { persistJob(job); persistInput(job, input) }
        defaultExecutor(this, timeout = { _, _ -> 1.milliseconds }).execute()
        val jobAfterComputation = fetchJob(job.uuid).right()
        jobAfterComputation.uuid shouldBeEqual job.uuid
        jobAfterComputation.type shouldBeEqual "TestJob"
        jobAfterComputation.tags.isEmpty()
        jobAfterComputation.customInfo.shouldBeNull()
        jobAfterComputation.priority.shouldBeZero()
        jobAfterComputation.createdBy shouldBeEqual defaultInstanceName
        jobAfterComputation.createdAt shouldBeEqual job.createdAt
        jobAfterComputation.status shouldBeEqual JobStatus.FAILURE
        jobAfterComputation.startedAt!!.shouldBeBetween(job.createdAt, now())
        jobAfterComputation.executingInstance!! shouldBeEqual defaultInstanceName
        jobAfterComputation.finishedAt!!.shouldBeBetween(jobAfterComputation.startedAt!!, now())
        jobAfterComputation.timeout!! shouldBeBefore now()
        jobAfterComputation.numRestarts.shouldBeZero()
        fetchFailure(job.uuid).right() shouldBeEqual "The job did not finish within the configured timeout of 1ms"
    }
})

private fun newJob() = Job(UUID.randomUUID().toString(), defaultJobType, emptyList(), null, 0, defaultInstanceName, now(), JobStatus.CREATED)
