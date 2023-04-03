// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.jobframework.boundary

import com.booleworks.jobframework.control.JobApiDef
import com.booleworks.jobframework.control.JobExecutor
import com.booleworks.jobframework.control.Maintenance
import com.booleworks.jobframework.control.scheduleForever
import com.booleworks.jobframework.control.setupJobApi
import com.booleworks.jobframework.data.DefaultJobPrioritizer
import com.booleworks.jobframework.data.ExecutionCapacity
import com.booleworks.jobframework.data.Job
import com.booleworks.jobframework.data.JobInput
import com.booleworks.jobframework.data.JobPrioritizer
import com.booleworks.jobframework.data.JobResult
import com.booleworks.jobframework.data.TagMatcher
import io.ktor.http.ContentType
import io.ktor.server.routing.Route
import io.ktor.server.routing.application
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

object JobFramework {
    fun <INPUT, RESULT, IN : JobInput<in INPUT>, RES : JobResult<out RESULT>> Route.newApi(
        persistence: Persistence<INPUT, RESULT, IN, RES>,
        myInstanceName: String,
        jobInputGenerator: (String) -> IN,
        failureGenerator: (String, String) -> RES,
        computation: suspend (Job, IN) -> RES?, // must only return null if the job (coroutine) was cancelled
        configuration: JobFrameworkBuilder<INPUT, RESULT, IN, RES>.() -> Unit
    ) = with(JobFrameworkBuilder(persistence, myInstanceName, computation, jobInputGenerator, failureGenerator).apply {
        configuration()
    }) { build() }
}

class JobFrameworkBuilder<INPUT, RESULT, IN : JobInput<in INPUT>, RES : JobResult<out RESULT>> internal constructor(
    private val persistence: Persistence<INPUT, RESULT, IN, RES>,
    private val myInstanceName: String,
    private val computation: suspend (Job, IN) -> RES?, // must only return null if the job (coroutine) was cancelled
    private val jobInputGenerator: (String) -> IN,
    private val failureGenerator: (String, String) -> RES,
) {
    private val apiConfig: ApiConfig = ApiConfig()
    private val validation: Validation<IN> = Validation()
    private val jobConfig: JobConfig<IN> = JobConfig()
    private val maintenanceConfig: MaintenanceConfig = MaintenanceConfig()
    private val cancellationConfig: CancellationConfig = CancellationConfig()

    fun apiConfig(block: ApiConfig.() -> Unit) = block(apiConfig)
    fun validation(block: Validation<IN>.() -> Unit) = block(validation)
    fun jobConfig(block: JobConfig<IN>.() -> Unit) = block(jobConfig)
    fun maintenanceConfig(block: MaintenanceConfig.() -> Unit) = block(maintenanceConfig)
    fun cancellationConfig(block: CancellationConfig.() -> Unit) = block(cancellationConfig)

    fun Route.build() {
        setupJobApi(generateJobApiDef())
        val executor = generateJobExecutor()
        application.scheduleForever(maintenanceConfig.jobCheckInterval) { executor.execute() }
        application.scheduleForever(maintenanceConfig.oldJobDeletionInterval) { Maintenance.deleteOldJobs(persistence, maintenanceConfig.deleteOldJobsAfter) }
        application.scheduleForever(maintenanceConfig.jobRestartCheckInterval) {
            Maintenance.restartLongRunningJobs(
                persistence,
                maintenanceConfig.maxJobRestarts,
                failureGenerator
            )
        }
        if (cancellationConfig.enableCancellation) {
            application.scheduleForever(cancellationConfig.cancellationCheckInterval) { Maintenance.checkForCancellations(persistence) }
        }
    }

    private fun generateJobApiDef() = JobApiDef(
        persistence,
        jobInputGenerator,
        myInstanceName,
        apiConfig.basePath,
        apiConfig.responseContentType,
        validation.inputValidation,
        jobConfig.tagProvider,
        jobConfig.customInfoProvider,
        jobConfig.priorityProvider,
        cancellationConfig.enableCancellation
    )

    private fun generateJobExecutor(): JobExecutor<INPUT, RESULT, IN, RES> = JobExecutor(
        persistence,
        myInstanceName,
        computation,
        jobConfig.executionCapacityProvider,
        jobConfig.timeoutComputation,
        jobConfig.jobPrioritizer,
        jobConfig.tagMatcher,
        failureGenerator
    )

    class ApiConfig {
        var basePath: String? = null
        var responseContentType: ContentType? = null
    }

    class Validation<IN> {
        var inputValidation: (IN) -> List<String> = { emptyList() }
    }

    class JobConfig<IN> {
        var tagProvider: (IN) -> List<String> = { emptyList() }
        var customInfoProvider: (IN) -> String = { "" }
        var priorityProvider: (IN) -> Int = { 0 }
        var executionCapacityProvider: (List<Job>) -> ExecutionCapacity =
            { if (it.isEmpty()) ExecutionCapacity.Companion.AcceptingAnyJob else ExecutionCapacity.Companion.AcceptingNoJob }
        var jobPrioritizer: JobPrioritizer = DefaultJobPrioritizer
        var tagMatcher: TagMatcher = TagMatcher.Any
        var timeoutComputation: (Job, IN) -> Duration = { _, _ -> 24.hours }
    }

    class MaintenanceConfig {
        var maxJobRestarts: Int = DEFAULT_MAX_JOB_RESTARTS
        var deleteOldJobsAfter: Duration = 365.days
        var jobCheckInterval: Duration = 5.seconds
        var jobRestartCheckInterval: Duration = 1.minutes
        var oldJobDeletionInterval: Duration = 1.days
    }

    class CancellationConfig {
        var enableCancellation: Boolean = false
        var cancellationCheckInterval: Duration = 1.seconds
    }

}

const val DEFAULT_MAX_JOB_RESTARTS = 3
