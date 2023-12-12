// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.executor

import com.booleworks.kjobs.api.JobFrameworkTestingApi
import com.booleworks.kjobs.api.JobFrameworkTestingMode
import com.booleworks.kjobs.api.persistence.hashmap.HashMapDataPersistence
import com.booleworks.kjobs.api.persistence.hashmap.HashMapJobPersistence
import com.booleworks.kjobs.common.TestInput
import com.booleworks.kjobs.common.TestResult
import com.booleworks.kjobs.common.defaultInstanceName
import com.booleworks.kjobs.common.expectSuccess
import com.booleworks.kjobs.common.reset
import com.booleworks.kjobs.common.setRunning
import com.booleworks.kjobs.common.shouldHaveBeenStarted
import com.booleworks.kjobs.control.ComputationResult
import com.booleworks.kjobs.data.DefaultExecutionCapacityProvider
import com.booleworks.kjobs.data.ExecutionCapacity
import com.booleworks.kjobs.data.ExecutionCapacityProvider
import com.booleworks.kjobs.data.Job
import com.booleworks.kjobs.data.JobStatus
import io.kotest.assertions.fail
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.equals.shouldBeEqual
import kotlinx.coroutines.CoroutineScope

class ExecutionCapacityTest : FunSpec({

    test("test default execution capacity provider") {
        val (jobPersistence, testingApi) = setupApi(executionCapacityProvider = DefaultExecutionCapacityProvider)
        val job1 = testingApi.submitJob("J1", TestInput(0)).expectSuccess()
        val job2 = testingApi.submitJob("J1", TestInput(42)).expectSuccess()
        jobPersistence.setRunning(job1.uuid)
        testingApi.runExecutor()
        jobPersistence.fetchJob(job2.uuid).expectSuccess().status shouldBeEqual JobStatus.CREATED

        testingApi.runExecutor(executionCapacityProvider = { ExecutionCapacity.Companion.AcceptingAnyJob })
        jobPersistence.fetchJob(job2.uuid).expectSuccess().shouldHaveBeenStarted()
    }

    test("test accepting any job") {
        val (jobPersistence, testingApi) = setupApi(executionCapacityProvider = { ExecutionCapacity.Companion.AcceptingAnyJob })
        val job1 = testingApi.submitJob("J1", TestInput(0)).expectSuccess()
        val job2 = testingApi.submitJob("J1", TestInput(42)).expectSuccess()
        val job3 = testingApi.submitJob("J1", TestInput(42)).expectSuccess()
        val job4 = testingApi.submitJob("J1", TestInput(42)).expectSuccess()
        jobPersistence.setRunning(job1.uuid)
        jobPersistence.setRunning(job2.uuid)
        jobPersistence.setRunning(job3.uuid)
        testingApi.runExecutor(executionCapacityProvider = { ExecutionCapacity.Companion.AcceptingNoJob })
        jobPersistence.fetchJob(job4.uuid).expectSuccess().status shouldBeEqual JobStatus.CREATED

        testingApi.runExecutor()
        jobPersistence.fetchJob(job4.uuid).expectSuccess().shouldHaveBeenStarted()
    }

    test("test accepting no job") {
        val (jobPersistence, testingApi) = setupApi(executionCapacityProvider = { ExecutionCapacity.Companion.AcceptingNoJob })
        val job1 = testingApi.submitJob("J1", TestInput(0)).expectSuccess()
        val job2 = testingApi.submitJob("J1", TestInput(42)).expectSuccess()
        val job3 = testingApi.submitJob("J1", TestInput(42)).expectSuccess()
        val job4 = testingApi.submitJob("J1", TestInput(42)).expectSuccess()
        jobPersistence.setRunning(job1.uuid)
        jobPersistence.setRunning(job2.uuid)
        jobPersistence.setRunning(job3.uuid)
        testingApi.runExecutor()
        jobPersistence.fetchJob(job4.uuid).expectSuccess().status shouldBeEqual JobStatus.CREATED

        testingApi.runExecutor(executionCapacityProvider = { ExecutionCapacity.Companion.AcceptingAnyJob })
        jobPersistence.fetchJob(job4.uuid).expectSuccess().shouldHaveBeenStarted()
    }

    test("test custom execution capacity") {
        val (jobPersistence, testingApi) = setupApi(executionCapacityProvider = {
            if (it.size > 2) {
                object : ExecutionCapacity {
                    override val mayTakeJobs = false
                    override fun isSufficientFor(job: Job): Boolean = fail("should not be called")
                }
            } else {
                object : ExecutionCapacity {
                    override val mayTakeJobs = true
                    override fun isSufficientFor(job: Job): Boolean = job.type == "J2"
                }
            }
        })
        val job1 = testingApi.submitJob("J1", TestInput(0)).expectSuccess()
        val job2 = testingApi.submitJob("J1", TestInput(42)).expectSuccess()
        val job3 = testingApi.submitJob("J1", TestInput(42)).expectSuccess()
        val job4 = testingApi.submitJob("J1", TestInput(42)).expectSuccess()
        testingApi.runExecutor()
        listOf(job1, job2, job3, job4).forEach { jobPersistence.fetchJob(it.uuid).expectSuccess().status shouldBeEqual JobStatus.CREATED }

        val job5 = testingApi.submitJob("J2", TestInput(42)).expectSuccess()
        repeat(2) {
            testingApi.runExecutor()
            listOf(job1, job2, job3, job4).forEach { jobPersistence.fetchJob(it.uuid).expectSuccess().status shouldBeEqual JobStatus.CREATED }
            jobPersistence.fetchJob(job5.uuid).expectSuccess().shouldHaveBeenStarted()
        }

        jobPersistence.reset(job5.uuid)
        jobPersistence.setRunning(job1.uuid)
        jobPersistence.setRunning(job2.uuid)
        jobPersistence.setRunning(job3.uuid)
        testingApi.runExecutor()
        listOf(job4, job5).forEach { jobPersistence.fetchJob(it.uuid).expectSuccess().status shouldBeEqual JobStatus.CREATED }

        val job6 = testingApi.submitJob("J1", TestInput(42)).expectSuccess()
        testingApi.submitJob("J2", TestInput(42)).expectSuccess()

        testingApi.runExecutor(executionCapacityProvider = {
            object : ExecutionCapacity {
                override val mayTakeJobs = true
                override fun isSufficientFor(job: Job) = job.uuid == job6.uuid
            }
        })
        jobPersistence.fetchJob(job6.uuid).expectSuccess().shouldHaveBeenStarted()
    }
})

private fun CoroutineScope.setupApi(executionCapacityProvider: ExecutionCapacityProvider): Pair<HashMapJobPersistence, JobFrameworkTestingApi> {
    val jobPersistence = HashMapJobPersistence()
    val j1Persistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
    val j2Persistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
    return jobPersistence to JobFrameworkTestingMode(defaultInstanceName, jobPersistence, false) {
        executorConfig { this.executionCapacityProvider = executionCapacityProvider }
        addJob("J1", j1Persistence, { _, _ -> ComputationResult.Success(TestResult(42)) })
        addJob("J2", j2Persistence, { _, _ -> ComputationResult.Success(TestResult(422)) })
    }
}
