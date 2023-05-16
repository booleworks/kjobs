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
import com.booleworks.kjobs.common.testBlocking
import com.booleworks.kjobs.control.ComputationResult
import com.booleworks.kjobs.data.TagMatcher
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.equals.shouldBeEqual
import io.kotest.matchers.maps.shouldBeEmpty
import io.kotest.matchers.maps.shouldHaveSize
import kotlinx.coroutines.CoroutineScope

class TagTest : FunSpec({

    testBlocking("test tagging") {
        val (testingApi, _, _) = setupApi()
        val job1 = testingApi.submitJob("J1", TestInput(0)).expectSuccess()
        job1.tags shouldBeEqual listOf("default_tag", "small_tag")
        val job2 = testingApi.submitJob("J1", TestInput(42)).expectSuccess()
        job2.tags shouldBeEqual listOf("default_tag", "large_tag")
        val job3 = testingApi.submitJob("J2", TestInput(0)).expectSuccess()
        job3.tags shouldBeEqual emptyList()
        val job4 = testingApi.submitJob("J2", TestInput(42)).expectSuccess()
        job4.tags shouldBeEqual listOf("large_tag")
    }

    testBlocking("test tag matching any") {
        testWithPredefinedAndOverriddenTagMatcher(TagMatcher.AllOf()) { testingApi, j1Persistence, j2Persistence, runExecutor ->
            testingApi.submitJob("J1", TestInput(0)).expectSuccess()
            testingApi.submitJob("J1", TestInput(42)).expectSuccess()
            testingApi.submitJob("J2", TestInput(0)).expectSuccess()
            testingApi.submitJob("J2", TestInput(42)).expectSuccess()
            j1Persistence.results.shouldBeEmpty()
            j2Persistence.results.shouldBeEmpty()
            runExecutor()
            j1Persistence.results shouldHaveSize 1
            j2Persistence.results.shouldBeEmpty()
            runExecutor()
            j1Persistence.results shouldHaveSize 2
            j2Persistence.results.shouldBeEmpty()
            runExecutor()
            j1Persistence.results shouldHaveSize 2
            j2Persistence.results shouldHaveSize 1
            runExecutor()
            j1Persistence.results shouldHaveSize 2
            j2Persistence.results shouldHaveSize 2
        }
    }

    testBlocking("test tag matching exactly") {
        val (testingApi, j1Persistence, j2Persistence) = setupApi()
        testingApi.submitJob("J1", TestInput(0)).expectSuccess() // j1, default_tag, small_tag
        testingApi.submitJob("J1", TestInput(42)).expectSuccess() // j2, default_tag, large_tag
        testingApi.submitJob("J2", TestInput(0)).expectSuccess() // j3, -
        testingApi.submitJob("J2", TestInput(42)).expectSuccess() // j4, large_tag
        j1Persistence.results.shouldBeEmpty()
        j2Persistence.results.shouldBeEmpty()
        testingApi.runExecutor(tagMatcher = TagMatcher.Exactly("different_tag"))
        j1Persistence.results.shouldBeEmpty()
        j2Persistence.results.shouldBeEmpty()
        testingApi.runExecutor(tagMatcher = TagMatcher.Exactly()) // j3
        j1Persistence.results.shouldBeEmpty()
        j2Persistence.results shouldHaveSize 1
        testingApi.runExecutor(tagMatcher = TagMatcher.Exactly())
        j1Persistence.results.shouldBeEmpty()
        j2Persistence.results shouldHaveSize 1
        testingApi.runExecutor(tagMatcher = TagMatcher.Exactly("small_tag", "default_tag", "different_tag"))
        j1Persistence.results.shouldBeEmpty()
        j2Persistence.results shouldHaveSize 1
        testingApi.runExecutor(tagMatcher = TagMatcher.Exactly("small_tag", "default_tag")) // j1
        j1Persistence.results shouldHaveSize 1
        j2Persistence.results shouldHaveSize 1
        testingApi.runExecutor(tagMatcher = TagMatcher.Exactly("small_tag", "default_tag"))
        j1Persistence.results shouldHaveSize 1
        j2Persistence.results shouldHaveSize 1
        testingApi.runExecutor(tagMatcher = TagMatcher.Exactly("default_tag"))
        j1Persistence.results shouldHaveSize 1
        j2Persistence.results shouldHaveSize 1
        testingApi.runExecutor(tagMatcher = TagMatcher.Exactly("large_tag")) // j4
        j1Persistence.results shouldHaveSize 1
        j2Persistence.results shouldHaveSize 2
        testingApi.runExecutor(tagMatcher = TagMatcher.Exactly("large_tag"))
        j1Persistence.results shouldHaveSize 1
        j2Persistence.results shouldHaveSize 2
        testingApi.runExecutor(tagMatcher = TagMatcher.Exactly("default_tag", "large_tag")) // j2
        j1Persistence.results shouldHaveSize 2
        j2Persistence.results shouldHaveSize 2
    }

    testBlocking("test tag matching oneOf") {
        val (testingApi, j1Persistence, j2Persistence) = setupApi()
        testingApi.submitJob("J1", TestInput(0)).expectSuccess() // j1, default_tag, small_tag
        testingApi.submitJob("J1", TestInput(42)).expectSuccess() // j2, default_tag, large_tag
        testingApi.submitJob("J2", TestInput(0)).expectSuccess() // j3, -
        testingApi.submitJob("J2", TestInput(42)).expectSuccess() // j4, large_tag
        j1Persistence.results.shouldBeEmpty()
        j2Persistence.results.shouldBeEmpty()
        testingApi.runExecutor(tagMatcher = TagMatcher.OneOf("different_tag"))
        j1Persistence.results.shouldBeEmpty()
        j2Persistence.results.shouldBeEmpty()
        testingApi.runExecutor(tagMatcher = TagMatcher.OneOf())
        j1Persistence.results.shouldBeEmpty()
        j2Persistence.results.shouldBeEmpty()
        testingApi.runExecutor(tagMatcher = TagMatcher.OneOf("small_tag", "default_tag", "different_tag")) // j1, j2
        j1Persistence.results shouldHaveSize 1
        j2Persistence.results.shouldBeEmpty()
        testingApi.runExecutor(tagMatcher = TagMatcher.OneOf("small_tag", "default_tag", "different_tag")) // j1, j2
        j1Persistence.results shouldHaveSize 2
        j2Persistence.results.shouldBeEmpty()
        testingApi.runExecutor(tagMatcher = TagMatcher.OneOf("small_tag", "default_tag", "different_tag"))
        j1Persistence.results shouldHaveSize 2
        j2Persistence.results.shouldBeEmpty()
        testingApi.runExecutor(tagMatcher = TagMatcher.OneOf("small_tag", "default_tag", "large_tag", "different_tag"))
        j1Persistence.results shouldHaveSize 2
        j2Persistence.results shouldHaveSize 1
    }

    testBlocking("test tag matching allOf") {
        val (testingApi, j1Persistence, j2Persistence) = setupApi()
        testingApi.submitJob("J1", TestInput(0)).expectSuccess() // j1, default_tag, small_tag
        testingApi.submitJob("J1", TestInput(42)).expectSuccess() // j2, default_tag, large_tag
        testingApi.submitJob("J2", TestInput(0)).expectSuccess() // j3, -
        testingApi.submitJob("J2", TestInput(42)).expectSuccess() // j4, large_tag
        j1Persistence.results.shouldBeEmpty()
        j2Persistence.results.shouldBeEmpty()
        testingApi.runExecutor(tagMatcher = TagMatcher.AllOf("different_tag"))
        j1Persistence.results.shouldBeEmpty()
        j2Persistence.results.shouldBeEmpty()
        testingApi.runExecutor(tagMatcher = TagMatcher.AllOf("small_tag", "large_tag"))
        j1Persistence.results.shouldBeEmpty()
        j2Persistence.results.shouldBeEmpty()
        testingApi.runExecutor(tagMatcher = TagMatcher.AllOf("small_tag", "default_tag")) // j1
        j1Persistence.results shouldHaveSize 1
        j2Persistence.results.shouldBeEmpty()
        testingApi.runExecutor(tagMatcher = TagMatcher.AllOf("small_tag", "default_tag")) // j1
        j1Persistence.results shouldHaveSize 1
        j2Persistence.results.shouldBeEmpty()
        testingApi.runExecutor(tagMatcher = TagMatcher.AllOf("large_tag")) // j2, j4
        j1Persistence.results shouldHaveSize 2
        j2Persistence.results.shouldBeEmpty()
        testingApi.runExecutor(tagMatcher = TagMatcher.AllOf("large_tag")) // j2, j4
        j1Persistence.results shouldHaveSize 2
        j2Persistence.results shouldHaveSize 1
        testingApi.runExecutor(tagMatcher = TagMatcher.AllOf(""))
        j1Persistence.results shouldHaveSize 2
        j2Persistence.results shouldHaveSize 1
        testingApi.runExecutor(tagMatcher = TagMatcher.AllOf())
        j1Persistence.results shouldHaveSize 2
        j2Persistence.results shouldHaveSize 2
    }

    testBlocking("test tag matching allOf with empty tags") {
        testWithPredefinedAndOverriddenTagMatcher(TagMatcher.AllOf()) { testingApi, j1Persistence, j2Persistence, runExecutor ->
            testingApi.submitJob("J1", TestInput(0)).expectSuccess() // j1, default_tag, small_tag
            testingApi.submitJob("J1", TestInput(42)).expectSuccess() // j2, default_tag, large_tag
            testingApi.submitJob("J2", TestInput(0)).expectSuccess() // j3, -
            testingApi.submitJob("J2", TestInput(42)).expectSuccess() // j4, large_tag
            j1Persistence.results.shouldBeEmpty()
            j2Persistence.results.shouldBeEmpty()
            runExecutor()
            j1Persistence.results shouldHaveSize 1
            j2Persistence.results.shouldBeEmpty()
            testingApi.runExecutor(tagMatcher = TagMatcher.AllOf())
            j1Persistence.results shouldHaveSize 2
            j2Persistence.results.shouldBeEmpty()
            testingApi.runExecutor(tagMatcher = TagMatcher.AllOf())
            j1Persistence.results shouldHaveSize 2
            j2Persistence.results shouldHaveSize 1
            testingApi.runExecutor(tagMatcher = TagMatcher.AllOf())
            j1Persistence.results shouldHaveSize 2
            j2Persistence.results shouldHaveSize 2
        }
    }
})

private fun CoroutineScope.testWithPredefinedAndOverriddenTagMatcher(
    tagMatcher: TagMatcher,
    block: (testingApi: JobFrameworkTestingApi, j1Persistence: HashMapDataPersistence<TestInput, TestResult>, j2Persistence: HashMapDataPersistence<TestInput, TestResult>, runExecutor: () -> Unit) -> Unit
) {
    val (testingApi, j1Persistence, j2Persistence) = setupApi(tagMatcher)
    block(testingApi, j1Persistence, j2Persistence) { testingApi.runExecutor() }
    val (testingApi1, j1Persistence1, j2Persistence1) = setupApi()
    block(testingApi1, j1Persistence1, j2Persistence1) { testingApi1.runExecutor(tagMatcher = tagMatcher) }
}

private fun CoroutineScope.setupApi(tagMatcher: TagMatcher? = null): Triple<JobFrameworkTestingApi, HashMapDataPersistence<TestInput, TestResult>, HashMapDataPersistence<TestInput, TestResult>> {
    val jobPersistence = HashMapJobPersistence()
    val j1Persistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
    val j2Persistence = HashMapDataPersistence<TestInput, TestResult>(jobPersistence)
    val testingApi = JobFrameworkTestingMode(defaultInstanceName, jobPersistence, Either.Left(this), false) {
        tagMatcher?.let { executorConfig { this.tagMatcher = tagMatcher } }
        addJob("J1", j1Persistence, { _, _ -> ComputationResult.Success(TestResult(42)) }) {
            jobConfig {
                tagProvider = { input -> if (input.value > 10) listOf("default_tag", "large_tag") else listOf("default_tag", "small_tag") }
            }
        }
        addJob("J2", j2Persistence, { _, _ -> ComputationResult.Success(TestResult(42)) }) {
            jobConfig {
                tagProvider = { input -> if (input.value > 10) listOf("large_tag") else emptyList() }
            }
        }
    }
    return Triple(testingApi, j1Persistence, j2Persistence)
}
