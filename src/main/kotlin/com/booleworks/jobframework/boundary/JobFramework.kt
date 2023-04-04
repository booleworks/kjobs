// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

// some imports are used in comments
@file:Suppress("UnusedImports")

package com.booleworks.jobframework.boundary

import com.booleworks.jobframework.boundary.JobFramework.newApi
import com.booleworks.jobframework.control.JobApiDef
import com.booleworks.jobframework.control.JobExecutor
import com.booleworks.jobframework.control.Maintenance
import com.booleworks.jobframework.control.scheduleForever
import com.booleworks.jobframework.control.setupJobApi
import com.booleworks.jobframework.data.DefaultExecutionCapacityProvider
import com.booleworks.jobframework.data.DefaultJobPrioritizer
import com.booleworks.jobframework.data.ExecutionCapacityProvider
import com.booleworks.jobframework.data.Job
import com.booleworks.jobframework.data.JobInput
import com.booleworks.jobframework.data.JobPrioritizer
import com.booleworks.jobframework.data.JobResult
import com.booleworks.jobframework.data.JobStatus
import com.booleworks.jobframework.data.TagMatcher
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.request.receiveText
import io.ktor.server.routing.Route
import io.ktor.server.routing.application
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * The method [newApi] is the central point to generate a new API.
 */
object JobFramework {

    /**
     * Generates a new API on the given [route]. The [configuration] is optional, but it is
     * strongly encouraged to take a look at what can further be configured there.
     *  @param route the route on which the API should be setup, it is also used to access
     *  the [ktor application's coroutine context][Application.coroutineContext]
     *  @param persistence the persistence/database to use for storing jobs, inputs, and results
     *  @param myInstanceName a unique identifier of this instance, e.g. in a Kubernetes environment.
     *  Can be an arbitrary, non-empty, string if there is only a single instance.
     *  @param jobInputGenerator a function which takes the input coming from [call.receiveText][ApplicationCall.receiveText]
     *  a generates a job input of type [IN]
     *  @param failureGenerator a function taking a uuid and an error message and returns a result
     *  @param computation the action computation which should be performed by this asynchronous service.
     *  It must only return `null` if it was cancelled.
     *  @param configuration provides more detailed configuration options as described below
     */
    fun <INPUT, RESULT, IN : JobInput<in INPUT>, RES : JobResult<out RESULT>> newApi(
        route: Route,
        persistence: Persistence<INPUT, RESULT, IN, RES>,
        myInstanceName: String,
        jobInputGenerator: (String) -> IN,
        failureGenerator: (String, String) -> RES,
        computation: suspend (Job, IN) -> RES?,
        configuration: JobFrameworkBuilder<INPUT, RESULT, IN, RES>.() -> Unit
    ): Unit = JobFrameworkBuilder(persistence, myInstanceName, computation, jobInputGenerator, failureGenerator).apply {
        configuration()
    }.build(route)
}

class JobFrameworkBuilder<INPUT, RESULT, IN : JobInput<in INPUT>, RES : JobResult<out RESULT>> internal constructor(
    private val persistence: Persistence<INPUT, RESULT, IN, RES>,
    private val myInstanceName: String,
    private val computation: suspend (Job, IN) -> RES?, // must only return null if the job (coroutine) was cancelled
    private val jobInputGenerator: (String) -> IN,
    private val failureGenerator: (String, String) -> RES,
) {
    private val apiConfig: ApiConfig<IN> = ApiConfig()
    private val jobConfig: JobConfig<IN> = JobConfig()
    private val maintenanceConfig: MaintenanceConfig = MaintenanceConfig()
    private val cancellationConfig: CancellationConfig = CancellationConfig()

    /**
     * Provides further configuration options about the API.
     */
    fun apiConfig(configuration: ApiConfig<IN>.() -> Unit) = configuration(apiConfig)

    /**
     * Provides further configuration options about the handling of jobs.
     */
    fun jobConfig(configuration: JobConfig<IN>.() -> Unit) = configuration(jobConfig)

    /**
     * Provides further configuration options about maintenance routines.
     */
    fun maintenanceConfig(configuration: MaintenanceConfig.() -> Unit) = configuration(maintenanceConfig)

    /**
     * Provides configuration options for the cancellation of jobs.
     */
    fun cancellationConfig(configuration: CancellationConfig.() -> Unit) = configuration(cancellationConfig)

    /**
     * Further configuration options for the API.
     * @param basePath an additional base path of the application (in addition to what is effectively predefined by the [Route] passed into [newApi]).
     * Default is the empty string.
     * @param responseContentType the desired content type for the result of the computation. Default is [ContentType.Text.Plain]
     * @param inputValidation an optional validation of the input which is performed in the `submit` resource. Must return a list of error messages which
     * is empty in case the validation did not find any errors. If the list is not empty, the request is rejected with [HttpStatusCode.NotFound] and
     * a message constructed from the list. Default is an empty list.
     */
    class ApiConfig<IN> internal constructor(
        var basePath: String? = null,
        var responseContentType: ContentType? = null,
        var inputValidation: (IN) -> List<String> = { emptyList() },
    )

    /**
     * Further configuration options about the handling of jobs.
     * @param tagProvider a function providing a list of tags (strings) for a job input. These tags are stored in [Job.tags]. Default is an empty list.
     * @param customInfoProvider a function providing a list of tags (strings) for a job input. These tags are stored in [Job.customInfo].
     * Default is an empty string.
     * @param priorityProvider a function providing an integer priority for a job input. A smaller number means a higher priority. Default is 0.
     * @param executionCapacityProvider an execution capacity provider, see [ExecutionCapacityProvider] for detailed information.
     * The [default provider][DefaultExecutionCapacityProvider] will allow at most one job running on an instance.
     * @param jobPrioritizer a job prioritizer, see [JobPrioritizer] for detailed information. The [default prioritizer][DefaultJobPrioritizer] will prioritize
     * first by [Job.priority] and then by [Job.createdAt] (both ascending).
     * @param tagMatcher a tag matcher to this instance to select only jobs with specific [tags][Job.tags]. Default is [TagMatcher.Any].
     * @param timeoutComputation a function providing a timeout for the given job. The default is 24 hours. In most cases this default should be set much lower.
     */
    class JobConfig<IN> internal constructor(
        var tagProvider: (IN) -> List<String> = { emptyList() },
        var customInfoProvider: (IN) -> String = { "" },
        var priorityProvider: (IN) -> Int = { 0 },
        var executionCapacityProvider: ExecutionCapacityProvider = DefaultExecutionCapacityProvider,
        var jobPrioritizer: JobPrioritizer = DefaultJobPrioritizer,
        var tagMatcher: TagMatcher = TagMatcher.Any,
        var timeoutComputation: (Job, IN) -> Duration = { _, _ -> 24.hours },
    )

    /**
     * Further configuration options about maintenance routines.
     *
     * @param maxJobRestarts the maximum number of restarts of a job. A job is restarted if its timeout is reached. Default is [DEFAULT_MAX_JOB_RESTARTS].
     * @param deleteOldJobsAfter the time after which finished jobs should be deleted. Default is 365 days. Usually, this value can be set much lower (e.g.
     * to one day or even less). Note that, depending on the number of requests and the size of the input and result, the job database may become very large if
     * this value is set too high.
     * @param jobCheckInterval the interval with which the instance should check for new jobs to compute
     * @param jobRestartCheckInterval the interval with which the instance should check for jobs which need to be restarted
     * @param oldJobDeletionInterval the interval with which the instance should check for old jobs to be deleted
     */
    class MaintenanceConfig internal constructor(
        var maxJobRestarts: Int = DEFAULT_MAX_JOB_RESTARTS,
        var deleteOldJobsAfter: Duration = 365.days,
        var jobCheckInterval: Duration = 5.seconds,
        var jobRestartCheckInterval: Duration = 1.minutes,
        var oldJobDeletionInterval: Duration = 1.days,
    )

    /**
     * Configuration options for the cancellation of jobs.
     *
     * [cancellationCheckInterval] is needed to update the set of jobs in status [JobStatus.CANCEL_REQUESTED] from the database. These are jobs which are
     * already running and will try to be aborted by cancelling their [coroutine job][kotlinx.coroutines.Job].
     *
     * @param enableCancellation whether cancellation is enabled or not. This will enable the resource `POST cancel/{uuid}`. Default is `false`.
     * @param cancellationCheckInterval the interval with which the instance should check for cancelled jobs. Default is 1 second. This value can be set much
     * higher depending on your needs (i.e. how urgent it is to abort a cancelled job which is still running).
     */
    class CancellationConfig internal constructor(
        var enableCancellation: Boolean = false,
        var cancellationCheckInterval: Duration = 1.seconds,
    )

    internal fun build(route: Route) = with(route) {
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
        apiConfig.inputValidation,
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
}

const val DEFAULT_MAX_JOB_RESTARTS = 3
