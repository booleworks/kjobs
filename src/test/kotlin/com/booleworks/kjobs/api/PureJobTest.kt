// SPDX-License-Identifier: MIT
// Copyright 2023-2024 BooleWorks GmbH

package com.booleworks.kjobs.api

import com.booleworks.kjobs.api.persistence.hashmap.HashMapDataPersistence
import com.booleworks.kjobs.api.persistence.hashmap.HashMapJobPersistence
import com.booleworks.kjobs.api.persistence.newJob
import com.booleworks.kjobs.common.TestInput
import com.booleworks.kjobs.common.TestResult
import com.booleworks.kjobs.common.defaultComputation
import com.booleworks.kjobs.common.defaultInstanceName
import com.booleworks.kjobs.common.defaultJobType
import com.booleworks.kjobs.common.expectSuccess
import com.booleworks.kjobs.data.JobStatus
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.equals.shouldBeEqual
import io.ktor.client.request.get
import io.ktor.server.testing.testApplication
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlin.time.Duration.Companion.milliseconds

class PureJobTest : FunSpec({

    test("test framework") {
        testApplication {
            val jobPersistence = HashMapJobPersistence()
            val dataPersistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
            var jobFramework: kotlinx.coroutines.Job? = null
            routing {
                jobFramework = JobFramework(defaultInstanceName, jobPersistence) {
                    addJob(defaultJobType, dataPersistence, defaultComputation)
                    maintenanceConfig { jobCheckInterval = 5.milliseconds }
                }
            }
            client.get("init-ktor-test-server")
            val job = newJob("42")
            dataPersistence.persistInput(job, TestInput(42))
            dataPersistence.persistJob(job)
            delay(20.milliseconds)
            dataPersistence.fetchJob("42").expectSuccess().status shouldBeEqual JobStatus.SUCCESS
            dataPersistence.fetchResult("42").expectSuccess() shouldBeEqual TestResult(42)

            val failing = newJob("43")
            dataPersistence.persistInput(failing, TestInput(42, throwException = true))
            dataPersistence.persistJob(failing)
            delay(20.milliseconds)
            dataPersistence.fetchJob("43").expectSuccess().status shouldBeEqual JobStatus.FAILURE
            dataPersistence.fetchFailure("43").expectSuccess() shouldBeEqual "Unexpected exception during computation: Test Exception Message"
            jobFramework!!.cancelAndJoin()
        }
    }

    test("test testing mode") {
        val jobPersistence = HashMapJobPersistence()
        val dataPersistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
        val testingApi = JobFrameworkTestingMode(defaultInstanceName, jobPersistence, false) {
            addJob(defaultJobType, dataPersistence, defaultComputation)
        }
        val job = newJob("42")
        dataPersistence.persistInput(job, TestInput(42))
        dataPersistence.persistJob(job)
        testingApi.runExecutor()
        dataPersistence.fetchJob("42").expectSuccess().status shouldBeEqual JobStatus.SUCCESS
        dataPersistence.fetchResult("42").expectSuccess() shouldBeEqual TestResult(42)

        val failing = newJob("43")
        dataPersistence.persistInput(failing, TestInput(42, throwException = true))
        dataPersistence.persistJob(failing)
        testingApi.runExecutor()
        dataPersistence.fetchJob("43").expectSuccess().status shouldBeEqual JobStatus.FAILURE
        dataPersistence.fetchFailure("43").expectSuccess() shouldBeEqual "Unexpected exception during computation: Test Exception Message"
    }
})
