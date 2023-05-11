// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.executor

import com.booleworks.kjobs.api.JobFrameworkTestingApi
import com.booleworks.kjobs.api.JobFrameworkTestingMode
import com.booleworks.kjobs.api.persistence.hashmap.HashMapDataPersistence
import com.booleworks.kjobs.api.persistence.hashmap.HashMapJobPersistence
import com.booleworks.kjobs.common.Either
import com.booleworks.kjobs.common.TestInput
import com.booleworks.kjobs.common.TestResult
import com.booleworks.kjobs.common.defaultInstanceName
import com.booleworks.kjobs.common.expectSuccess
import com.booleworks.kjobs.control.ComputationResult
import com.booleworks.kjobs.data.Job
import com.booleworks.kjobs.data.JobPrioritizer
import com.booleworks.kjobs.data.JobStatus
import com.booleworks.kjobs.data.JobStatus.CREATED
import com.booleworks.kjobs.data.JobStatus.SUCCESS
import io.kotest.common.runBlocking
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.equals.shouldBeEqual
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.coroutineScope

class PriorityTest : FunSpec({
    test("test priority setting") {
        runBlocking {
            coroutineScope {
                val testingApi = setupApi()
                val job1 = testingApi.submitJob("J1", TestInput(0)).expectSuccess()
                val job2 = testingApi.submitJob("J1", TestInput(42)).expectSuccess()
                val job3 = testingApi.submitJob("J2", TestInput(5)).expectSuccess()
                val job4 = testingApi.submitJob("J2", TestInput(15)).expectSuccess()
                job1.priority shouldBeEqual 10
                job2.priority shouldBeEqual 20
                job3.priority shouldBeEqual 5
                job4.priority shouldBeEqual 15
            }
        }
    }

    test("test default priority-based job selection") {
        runBlocking {
            coroutineScope {
                val testingApi = setupApi()
                val job1 = testingApi.submitJob("J1", TestInput(0)).expectSuccess()
                val job2 = testingApi.submitJob("J1", TestInput(42)).expectSuccess()
                val job3 = testingApi.submitJob("J2", TestInput(5)).expectSuccess()
                val job4 = testingApi.submitJob("J2", TestInput(15)).expectSuccess()

                testingApi.runExecutor()
                job3 shouldHaveStatus SUCCESS
                listOf(job1, job2, job4) shouldHaveStatus CREATED

                testingApi.runExecutor()
                job1 shouldHaveStatus SUCCESS
                listOf(job2, job4) shouldHaveStatus CREATED

                testingApi.runExecutor()
                job4 shouldHaveStatus SUCCESS
                job2 shouldHaveStatus CREATED

                testingApi.runExecutor()
                job2 shouldHaveStatus SUCCESS
            }
        }
    }

    test("test job selection with custom job prioritizer") {
        runBlocking {
            coroutineScope {
                val testingApi = setupApi { jobs -> jobs.maxByOrNull { it.priority } }
                val job1 = testingApi.submitJob("J1", TestInput(0)).expectSuccess()
                val job2 = testingApi.submitJob("J1", TestInput(42)).expectSuccess()
                val job3 = testingApi.submitJob("J2", TestInput(5)).expectSuccess()
                val job4 = testingApi.submitJob("J2", TestInput(15)).expectSuccess()

                testingApi.runExecutor()
                job2 shouldHaveStatus SUCCESS
                listOf(job1, job3, job4) shouldHaveStatus CREATED

                testingApi.runExecutor()
                job4 shouldHaveStatus SUCCESS
                listOf(job1, job3) shouldHaveStatus CREATED

                testingApi.runExecutor()
                job1 shouldHaveStatus SUCCESS
                job3 shouldHaveStatus CREATED

                testingApi.runExecutor()
                job3 shouldHaveStatus SUCCESS
            }
        }
    }

    test("test job selection with overridden priority") {
        runBlocking {
            coroutineScope {
                val testingApi = setupApi { jobs -> jobs.maxByOrNull { it.priority } }
                val job1 = testingApi.submitJob("J1", TestInput(0)).expectSuccess()
                val job2 = testingApi.submitJob("J1", TestInput(42)).expectSuccess()
                val job3 = testingApi.submitJob("J2", TestInput(5)).expectSuccess()
                val job4 = testingApi.submitJob("J2", TestInput(15)).expectSuccess()

                testingApi.runExecutor(jobPrioritizer = { jobs -> jobs.maxByOrNull { it.createdAt } })
                job4 shouldHaveStatus SUCCESS
                listOf(job1, job2, job3) shouldHaveStatus CREATED

                testingApi.runExecutor(jobPrioritizer = { jobs -> jobs.find { it.type == "J2" } })
                job3 shouldHaveStatus SUCCESS
                listOf(job1, job2) shouldHaveStatus CREATED

                testingApi.runExecutor()
                job2 shouldHaveStatus SUCCESS
                job1 shouldHaveStatus CREATED

                testingApi.runExecutor()
                job1 shouldHaveStatus SUCCESS
            }
        }
    }
})

private fun CoroutineScope.setupApi(jobPrioritizer: JobPrioritizer? = null): JobFrameworkTestingApi {
    val jobPersistence = HashMapJobPersistence()
    val j1Persistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
    val j2Persistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
    val testingApi = JobFrameworkTestingMode(defaultInstanceName, jobPersistence, Either.Left(this), false) {
        jobPrioritizer?.let { executorConfig { this.jobPrioritizer = jobPrioritizer } }
        addJob("J1", j1Persistence, { _, _ -> ComputationResult.Success(TestResult(42)) }) {
            jobConfig {
                priorityProvider = { input -> if (input.value > 10) 20 else 10 }
            }
        }
        addJob("J2", j2Persistence, { _, _ -> ComputationResult.Success(TestResult(42)) }) {
            jobConfig {
                priorityProvider = TestInput::value
            }
        }
    }
    return testingApi
}

private infix fun Job.shouldHaveStatus(status: JobStatus) {
    this.status shouldBeEqual status
}

private infix fun Collection<Job>.shouldHaveStatus(status: JobStatus) = this.forEach { it shouldHaveStatus status }
