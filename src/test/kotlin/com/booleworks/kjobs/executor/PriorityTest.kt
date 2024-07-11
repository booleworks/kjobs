// SPDX-License-Identifier: MIT
// Copyright 2023-2024 BooleWorks GmbH

package com.booleworks.kjobs.executor

import com.booleworks.kjobs.api.JobFrameworkTestingApi
import com.booleworks.kjobs.api.JobFrameworkTestingMode
import com.booleworks.kjobs.api.persistence.hashmap.HashMapDataPersistence
import com.booleworks.kjobs.api.persistence.hashmap.HashMapJobPersistence
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
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.equals.shouldBeEqual

class PriorityTest : FunSpec({

    test("test priority setting") {
        val (testingApi, _) = setupApi()
        val job1 = testingApi.submitJob("J1", TestInput(0)).expectSuccess()
        val job2 = testingApi.submitJob("J1", TestInput(42)).expectSuccess()
        val job3 = testingApi.submitJob("J2", TestInput(5)).expectSuccess()
        val job4 = testingApi.submitJob("J2", TestInput(15)).expectSuccess()
        job1.priority shouldBeEqual 10
        job2.priority shouldBeEqual 20
        job3.priority shouldBeEqual 5
        job4.priority shouldBeEqual 15
    }

    test("test default priority-based job selection") {
        val (testingApi, persistence) = setupApi()
        val job1 = testingApi.submitJob("J1", TestInput(0)).expectSuccess().uuid
        val job2 = testingApi.submitJob("J1", TestInput(42)).expectSuccess().uuid
        val job3 = testingApi.submitJob("J2", TestInput(5)).expectSuccess().uuid
        val job4 = testingApi.submitJob("J2", TestInput(15)).expectSuccess().uuid

        testingApi.runExecutor()
        listOf(job3).map { persistence.fetchJob(it).expectSuccess() } shouldHaveStatus SUCCESS
        listOf(job1, job2, job4).map { persistence.fetchJob(it).expectSuccess() } shouldHaveStatus CREATED

        testingApi.runExecutor()
        listOf(job1).map { persistence.fetchJob(it).expectSuccess() } shouldHaveStatus SUCCESS
        listOf(job2, job4).map { persistence.fetchJob(it).expectSuccess() } shouldHaveStatus CREATED

        testingApi.runExecutor()
        listOf(job4).map { persistence.fetchJob(it).expectSuccess() } shouldHaveStatus SUCCESS
        listOf(job2).map { persistence.fetchJob(it).expectSuccess() } shouldHaveStatus CREATED

        testingApi.runExecutor()
        listOf(job2).map { persistence.fetchJob(it).expectSuccess() } shouldHaveStatus SUCCESS
    }

    test("test job selection with custom job prioritizer") {
        val (testingApi, persistence) = setupApi { jobs -> jobs.maxByOrNull { it.priority } }
        val job1 = testingApi.submitJob("J1", TestInput(0)).expectSuccess().uuid
        val job2 = testingApi.submitJob("J1", TestInput(42)).expectSuccess().uuid
        val job3 = testingApi.submitJob("J2", TestInput(5)).expectSuccess().uuid
        val job4 = testingApi.submitJob("J2", TestInput(15)).expectSuccess().uuid

        testingApi.runExecutor()
        listOf(job2).map { persistence.fetchJob(it).expectSuccess() } shouldHaveStatus SUCCESS
        listOf(job1, job3, job4).map { persistence.fetchJob(it).expectSuccess() } shouldHaveStatus CREATED

        testingApi.runExecutor()
        listOf(job4).map { persistence.fetchJob(it).expectSuccess() } shouldHaveStatus SUCCESS
        listOf(job1, job3).map { persistence.fetchJob(it).expectSuccess() } shouldHaveStatus CREATED

        testingApi.runExecutor()
        listOf(job1).map { persistence.fetchJob(it).expectSuccess() } shouldHaveStatus SUCCESS
        listOf(job3).map { persistence.fetchJob(it).expectSuccess() } shouldHaveStatus CREATED

        testingApi.runExecutor()
        listOf(job3).map { persistence.fetchJob(it).expectSuccess() } shouldHaveStatus SUCCESS
    }

    test("test job selection with overridden priority") {
        val (testingApi, persistence) = setupApi { jobs -> jobs.maxByOrNull { it.priority } }
        val job1 = testingApi.submitJob("J1", TestInput(0)).expectSuccess().uuid
        val job2 = testingApi.submitJob("J1", TestInput(42)).expectSuccess().uuid
        val job3 = testingApi.submitJob("J2", TestInput(5)).expectSuccess().uuid
        val job4 = testingApi.submitJob("J2", TestInput(15)).expectSuccess().uuid

        testingApi.runExecutor(jobPrioritizer = { jobs -> jobs.maxByOrNull { it.createdAt } })
        listOf(job4).map { persistence.fetchJob(it).expectSuccess() } shouldHaveStatus SUCCESS
        listOf(job1, job2, job3).map { persistence.fetchJob(it).expectSuccess() } shouldHaveStatus CREATED

        testingApi.runExecutor(jobPrioritizer = { jobs -> jobs.find { it.type == "J2" } })
        listOf(job3).map { persistence.fetchJob(it).expectSuccess() } shouldHaveStatus SUCCESS
        listOf(job1, job2).map { persistence.fetchJob(it).expectSuccess() } shouldHaveStatus CREATED

        testingApi.runExecutor()
        listOf(job2).map { persistence.fetchJob(it).expectSuccess() } shouldHaveStatus SUCCESS
        listOf(job1).map { persistence.fetchJob(it).expectSuccess() } shouldHaveStatus CREATED

        testingApi.runExecutor()
        listOf(job1).map { persistence.fetchJob(it).expectSuccess() } shouldHaveStatus SUCCESS
    }
})

private fun setupApi(jobPrioritizer: JobPrioritizer? = null): Pair<JobFrameworkTestingApi, HashMapJobPersistence> {
    val jobPersistence = HashMapJobPersistence()
    val j1Persistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
    val j2Persistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
    val testingApi = JobFrameworkTestingMode(defaultInstanceName, jobPersistence, false) {
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
    return testingApi to jobPersistence
}

private infix fun Job.shouldHaveStatus(status: JobStatus) {
    this.status shouldBeEqual status
}

private infix fun Collection<Job>.shouldHaveStatus(status: JobStatus) = this.forEach { it shouldHaveStatus status }
