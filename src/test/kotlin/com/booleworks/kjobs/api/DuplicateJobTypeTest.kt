// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.api

import com.booleworks.kjobs.api.hierarchical.SubTestInput1
import com.booleworks.kjobs.api.hierarchical.SubTestInput2
import com.booleworks.kjobs.api.hierarchical.SubTestResult1
import com.booleworks.kjobs.api.hierarchical.SubTestResult2
import com.booleworks.kjobs.api.hierarchical.subJob1
import com.booleworks.kjobs.api.hierarchical.subJob2
import com.booleworks.kjobs.api.hierarchical.superComputation
import com.booleworks.kjobs.common.TestInput
import com.booleworks.kjobs.common.TestResult
import com.booleworks.kjobs.common.defaultComputation
import com.booleworks.kjobs.common.defaultInstanceName
import com.booleworks.kjobs.common.defaultJobType
import com.booleworks.kjobs.common.defaultRedis
import com.booleworks.kjobs.common.newRedisPersistence
import com.booleworks.kjobs.common.testJobFrameworkWithRedis
import com.booleworks.kjobs.control.ComputationResult
import com.booleworks.kjobs.data.ExecutionCapacity
import com.booleworks.kjobs.data.ExecutionCapacityProvider
import io.kotest.assertions.throwables.shouldThrowWithMessage
import io.kotest.core.spec.style.FunSpec
import io.ktor.server.application.call
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.route
import kotlin.time.Duration.Companion.milliseconds

class DuplicateJobTypeTest : FunSpec({

    testJobFrameworkWithRedis("test add duplicate APIs") {
        val persistence = newRedisPersistence<TestInput, TestResult>(defaultRedis)
        routing {
            shouldThrowWithMessage<IllegalArgumentException>("An API or job with type type1 is already defined") {
                JobFramework(defaultInstanceName, persistence) {
                    maintenanceConfig { jobCheckInterval = 500.milliseconds }
                    route("test1") {
                        addApi("type1", this@route, persistence, { call.receive<TestInput>() }, { call.respond<TestResult>(it) }, defaultComputation)
                    }
                    route("test2") {
                        addApi("type1", this@route, persistence, { call.receive<TestInput>() }, { call.respond<TestResult>(it) }, defaultComputation)
                    }
                }
            }
        }
    }

    testJobFrameworkWithRedis("test add duplicate jobs") {
        val persistence = newRedisPersistence<TestInput, TestResult>(defaultRedis)
        routing {
            shouldThrowWithMessage<IllegalArgumentException>("An API or job with type type1 is already defined") {
                JobFramework(defaultInstanceName, persistence) {
                    maintenanceConfig { jobCheckInterval = 500.milliseconds }
                    addJob("type1", persistence, defaultComputation)
                    addJob("type1", persistence, defaultComputation)
                }
            }
        }
    }

    testJobFrameworkWithRedis("test add duplicate APIs and jobs") {
        val persistence = newRedisPersistence<TestInput, TestResult>(defaultRedis)
        routing {
            shouldThrowWithMessage<IllegalArgumentException>("An API or job with type type1 is already defined") {
                JobFramework(defaultInstanceName, persistence) {
                    maintenanceConfig { jobCheckInterval = 500.milliseconds }
                    route("test1") {
                        addApi("type1", this@route, persistence, { call.receive<TestInput>() }, { call.respond<TestResult>(it) }, defaultComputation)
                    }
                    addJob("type1", persistence, defaultComputation)
                }
            }
            shouldThrowWithMessage<IllegalArgumentException>("An API or job with type type1 is already defined") {
                JobFramework(defaultInstanceName, persistence) {
                    maintenanceConfig { jobCheckInterval = 500.milliseconds }
                    addJob("type1", persistence, defaultComputation)
                    route("test1") {
                        addApi("type1", this@route, persistence, { call.receive<TestInput>() }, { call.respond<TestResult>(it) }, defaultComputation)
                    }
                }
            }
        }
    }

    testJobFrameworkWithRedis("test hierarchical jobs") {
        val persistence = newRedisPersistence<TestInput, TestResult>()
        routing {
            JobFramework(defaultInstanceName, persistence) {
                route("test1") {
                    addApi(
                        subJob1,
                        this,
                        newRedisPersistence<SubTestInput1, SubTestResult1>(),
                        { call.receive<SubTestInput1>() },
                        { call.respond<SubTestResult1>(it) },
                        { _, _ -> ComputationResult.Success(SubTestResult1(1)) })
                }
                shouldThrowWithMessage<IllegalArgumentException>("An API or job with type $subJob1 is already defined") {
                    route("test2") {
                        maintenanceConfig { jobCheckInterval = 20.milliseconds }
                        executorConfig { executionCapacityProvider = ExecutionCapacityProvider { ExecutionCapacity.Companion.AcceptingAnyJob } }
                        addApiForHierarchicalJob(
                            defaultJobType, this@route, persistence,
                            { call.receive<TestInput>() }, { call.respond<TestResult>(it) }, superComputation()
                        ) {
                            addDependentJob(
                                subJob1,
                                newRedisPersistence<SubTestInput1, SubTestResult1>(),
                                { _, _ -> ComputationResult.Success(SubTestResult1(1)) }) {}
                            addDependentJob(
                                subJob2,
                                newRedisPersistence<SubTestInput2, SubTestResult2>(),
                                { _, _ -> ComputationResult.Success(SubTestResult2(1)) }) {}
                            shouldThrowWithMessage<IllegalArgumentException>("A dependent job with type $subJob1 is already defined") {
                                addDependentJob(
                                    subJob1,
                                    newRedisPersistence<SubTestInput1, SubTestResult1>(),
                                    { _, _ -> ComputationResult.Success(SubTestResult1(1)) }) {}
                            }
                        }
                    }
                }
            }
        }
    }

    testJobFrameworkWithRedis("test hierarchical jobs 2") {
        val persistence = newRedisPersistence<TestInput, TestResult>()
        routing {
            JobFramework(defaultInstanceName, persistence) {
                route("test1") {
                    maintenanceConfig { jobCheckInterval = 20.milliseconds }
                    executorConfig { executionCapacityProvider = ExecutionCapacityProvider { ExecutionCapacity.Companion.AcceptingAnyJob } }
                    addApiForHierarchicalJob(
                        defaultJobType, this@route, persistence,
                        { call.receive<TestInput>() }, { call.respond<TestResult>(it) }, superComputation()
                    ) {
                        addDependentJob(
                            subJob1,
                            newRedisPersistence<SubTestInput1, SubTestResult1>(),
                            { _, _ -> ComputationResult.Success(SubTestResult1(1)) }) {}
                        addDependentJob(
                            subJob2,
                            newRedisPersistence<SubTestInput2, SubTestResult2>(),
                            { _, _ -> ComputationResult.Success(SubTestResult2(1)) }) {}
                    }
                }
                route("test2") {
                    shouldThrowWithMessage<IllegalArgumentException>("An API or job with type $subJob1 is already defined") {
                        addJob(subJob1, newRedisPersistence<SubTestInput1, SubTestResult1>(), { _, _ -> ComputationResult.Success(SubTestResult1(1)) })
                    }
                }
            }
        }
    }
})
