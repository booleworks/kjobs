// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.api

import com.booleworks.kjobs.api.hierarchical.HierarchicalJobApi
import com.booleworks.kjobs.api.hierarchical.HierarchicalJobApiImpl
import com.booleworks.kjobs.api.persistence.DataPersistence
import com.booleworks.kjobs.api.persistence.JobPersistence
import com.booleworks.kjobs.control.ApiConfig
import com.booleworks.kjobs.control.ComputationResult
import com.booleworks.kjobs.control.JobConfig
import com.booleworks.kjobs.control.JobInfoConfig
import com.booleworks.kjobs.control.MainJobExecutor
import com.booleworks.kjobs.control.Maintenance
import com.booleworks.kjobs.control.SpecificExecutor
import com.booleworks.kjobs.control.SynchronousResourceConfig
import com.booleworks.kjobs.control.scheduleForever
import com.booleworks.kjobs.control.setupJobApi
import com.booleworks.kjobs.data.DefaultExecutionCapacityProvider
import com.booleworks.kjobs.data.DefaultJobPrioritizer
import com.booleworks.kjobs.data.ExecutionCapacityProvider
import com.booleworks.kjobs.data.Job
import com.booleworks.kjobs.data.JobPrioritizer
import com.booleworks.kjobs.data.JobStatus
import com.booleworks.kjobs.data.TagMatcher
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.call
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.util.pipeline.PipelineContext
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import java.util.concurrent.Executors
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@DslMarker
annotation class KJobsDsl

/**
 * Main entry point to set up the job framework from an application.
 * @param myInstanceName a unique identifier of this instance, e.g. in a Kubernetes environment.
 * Can be an arbitrary, non-empty, string if there is only a single instance.
 * @param jobPersistence an object allowing to store and retrieve jobs (and heartbeats) from some
 * (usually external) data source, e.g. a redis cache or a postgres database
 * @param configuration additional configuration options, in particular this includes the
 * possibility to [add APIs][JobFrameworkBuilder.addApi] to a ktor application
 */
@Suppress("FunctionName")
@KJobsDsl
fun JobFramework(
    myInstanceName: String,
    jobPersistence: JobPersistence,
    configuration: JobFrameworkBuilder.() -> Unit
) = JobFrameworkBuilder(myInstanceName, jobPersistence).apply(configuration).build()

/**
 * Builder for the KJobs Job Framework. Can be instantiated and configured using [JobFramework].
 */
@KJobsDsl
class JobFrameworkBuilder internal constructor(
    private val myInstanceName: String,
    private val jobPersistence: JobPersistence,
    private val maintenanceEnabled: Boolean = true
) {
    private val executorConfig: ExecutorConfig = ExecutorConfig()
    private val maintenanceConfig: MaintenanceConfig = MaintenanceConfig()
    private val cancellationConfig: CancellationConfig = CancellationConfig()
    private val apis: MutableMap<String, ApiBuilder<*, *>> = mutableMapOf()
    private val jobs: MutableMap<String, JobBuilder<*, *>> = mutableMapOf()
    private val executorsPerType: MutableMap<String, SpecificExecutor<*, *>> = mutableMapOf()
    private val persistencesPerType: MutableMap<String, DataPersistence<*, *>> = mutableMapOf()
    private val restartsPerType: MutableMap<String, Int> = mutableMapOf()

    /**
     * Generates a new API for a specific [job type][jobType]. The [configuration] is optional, but it is
     * strongly encouraged to take a look at what can further be configured there.
     * @param jobType a unique name identifying this type of job
     * @param route the route on which the API should be setup
     * @param dataPersistence the persistence/database to use for storing the inputs and results
     * for this specific job type
     * @param inputReceiver a function which receives the [INPUT] in the ktor webservice,
     * e.g. via `call.receive()`
     * @param resultResponder a function which returns the [RESULT] in the ktor webservice,
     * e.g. via `call.respond(result)`
     * @param computation the actual computation which should be performed by this asynchronous service
     * @param configuration provides more detailed configuration options as described below
     */
    fun <INPUT, RESULT> addApi(
        jobType: String,
        route: Route,
        dataPersistence: DataPersistence<INPUT, RESULT>,
        inputReceiver: suspend PipelineContext<Unit, ApplicationCall>.() -> INPUT,
        resultResponder: suspend PipelineContext<Unit, ApplicationCall>.(RESULT) -> Unit,
        computation: suspend (Job, INPUT) -> ComputationResult<RESULT>,
        configuration: ApiBuilder<INPUT, RESULT>.() -> Unit = {}
    ): ApiBuilder<INPUT, RESULT> {
        checkForDuplicateType(jobType)
        return ApiBuilder(myInstanceName, jobType, route, dataPersistence, inputReceiver, resultResponder).apply {
            configuration()
            this.computation = computation
            this@JobFrameworkBuilder.executorsPerType[jobType] = specificExecutor()
            this@JobFrameworkBuilder.persistencesPerType[jobType] = dataPersistence
            this@JobFrameworkBuilder.restartsPerType[jobType] = jobConfig.maxRestarts
            this@JobFrameworkBuilder.apis[jobType] = this
        }
    }

    /**
     * Generates a new API for a hierarchical job.
     *
     * *This is an experimental API which might change in the future! It should also be noted that especially the
     * implementation of the [parentComputation] (which has to be provided by the library user) is not trivial.*
     *
     * A hierarchical job is a job which can submit further "dependent" jobs. Essentially, these dependent
     * jobs are handled the same way as "normal" jobs added with [addApi].
     *
     * A hierarchical job must have one or more dependent jobs which are set up in the [configuration] via
     * [HierarchicalApiBuilder.addDependentJob] (for many cases it will be enough to have one type of dependent
     * job, but there can be more). The [parentComputation] slightly differs from the computation in
     * [a simple API][addApi] in that it takes (additionally to the [Job] and [INPUT]) a map from dependent job
     * type to [HierarchicalJobApi].
     *
     * The [HierarchicalJobApi] is used to actually [submit dependent jobs][HierarchicalJobApi.submitDependentJob]
     * and [collect their results][HierarchicalJobApi.collectDependentResults].
     *
     * **Note that you might need to adjust the [ExecutionCapacityProvider] of your instance via [executorConfig],
     * since the [parent computation][parentComputation] may spend a lot of time just waiting for its dependent jobs
     * to finish.** Especially sticking with the [DefaultExecutionCapacityProvider] in a single-instance environment
     * will essentially lead to a deadlock, since it allows only a single running job. So since the parent
     * computation is already running, the instance will not be able to start any of its dependent jobs.
     * @param jobType a unique name identifying this type of job
     * @param route the route on which the API should be setup
     * @param dataPersistence the persistence/database to use for storing the inputs and results
     * for this specific job type
     * @param inputReceiver a function which receives the [INPUT] in the ktor webservice,
     * e.g. via `call.receive()`
     * @param resultResponder a function which returns the [RESULT] in the ktor webservice,
     * e.g. via `call.respond(result)`
     * @param parentComputation the parent computation
     * @param configuration provides more detailed configuration options as described below
     */
    fun <INPUT, RESULT> addApiForHierarchicalJob(
        jobType: String,
        route: Route,
        dataPersistence: DataPersistence<INPUT, RESULT>,
        inputReceiver: suspend PipelineContext<Unit, ApplicationCall>.() -> INPUT,
        resultResponder: suspend PipelineContext<Unit, ApplicationCall>.(RESULT) -> Unit,
        parentComputation: suspend (Job, INPUT, Map<String, HierarchicalJobApi<*, *>>) -> ComputationResult<RESULT>,
        configuration: HierarchicalApiBuilder<INPUT, RESULT>.() -> Unit = {}
    ): HierarchicalApiBuilder<INPUT, RESULT> {
        checkForDuplicateType(jobType)
        return HierarchicalApiBuilder(myInstanceName, jobType, route, dataPersistence, inputReceiver, resultResponder, parentComputation).apply {
            configuration()
            dependents.keys.forEach { this@JobFrameworkBuilder.checkForDuplicateType(it) }
            this.computation = { job, input -> superComputation(job, input, dependents.mapValues { HierarchicalJobApiImpl(it.value.first, job.uuid) }) }
            this@JobFrameworkBuilder.executorsPerType[jobType] = specificExecutor()
            this@JobFrameworkBuilder.persistencesPerType[jobType] = dataPersistence
            this@JobFrameworkBuilder.restartsPerType[jobType] = jobConfig.maxRestarts
            this@JobFrameworkBuilder.executorsPerType += dependents.mapValues { it.value.second }
            this@JobFrameworkBuilder.persistencesPerType += dependents.mapValues { it.value.third }
            this@JobFrameworkBuilder.apis[jobType] = this
        }
    }

    /**
     * Adds a job to the framework without adding routes. This usually only makes sense in testing mode.
     * @param jobType a unique name identifying this type of job
     * @param dataPersistence the persistence/database to use for storing the inputs and results
     * for this specific job type
     * @param computation the actual computation which should be performed by this asynchronous service
     * @param configuration provides more detailed configuration options as described below
     */
    fun <INPUT, RESULT> addJob(
        jobType: String,
        dataPersistence: DataPersistence<INPUT, RESULT>,
        computation: suspend (Job, INPUT) -> ComputationResult<RESULT>,
        configuration: JobBuilder<INPUT, RESULT>.() -> Unit = {}
    ): JobBuilder<INPUT, RESULT> {
        checkForDuplicateType(jobType)
        return JobBuilder(myInstanceName, dataPersistence, computation).apply {
            configuration()
            this@JobFrameworkBuilder.executorsPerType[jobType] = specificExecutor()
            this@JobFrameworkBuilder.persistencesPerType[jobType] = dataPersistence
            this@JobFrameworkBuilder.restartsPerType[jobType] = jobConfig.maxRestarts
            this@JobFrameworkBuilder.jobs[jobType] = this
        }
    }

    /**
     * Provides further configuration options about the executor.
     */
    fun executorConfig(configuration: ExecutorConfig.() -> Unit) = configuration(executorConfig)

    /**
     * Provides further configuration options about maintenance routines.
     */
    fun maintenanceConfig(configuration: MaintenanceConfig.() -> Unit) = configuration(maintenanceConfig)

    /**
     * Provides configuration options for the cancellation of jobs.
     */
    fun cancellationConfig(configuration: CancellationConfig.() -> Unit) = configuration(cancellationConfig)

    /**
     * Further configuration options for the executor
     * @param executionCapacityProvider an execution capacity provider, see [ExecutionCapacityProvider] for detailed information.
     * The [default provider][DefaultExecutionCapacityProvider] will allow at most one job running on an instance.
     * @param jobPrioritizer a job prioritizer, see [JobPrioritizer] for detailed information. The [default prioritizer][DefaultJobPrioritizer] will prioritize
     * first by [Job.priority] and then by [Job.createdAt] (both ascending).
     * @param tagMatcher a tag matcher to this instance to select only jobs with specific [tags][Job.tags]. Default is [TagMatcher.Any].
     * @param dispatcher the [CoroutineDispatcher] in which the computations are running. By default, this is [Dispatchers.Default].
     */
    @KJobsDsl
    class ExecutorConfig internal constructor(
        var executionCapacityProvider: ExecutionCapacityProvider = DefaultExecutionCapacityProvider,
        var jobPrioritizer: JobPrioritizer = DefaultJobPrioritizer,
        var tagMatcher: TagMatcher = TagMatcher.Any,
        var dispatcher: CoroutineDispatcher = Dispatchers.Default
    )

    /**
     * Further configuration options about maintenance routines.
     *
     * @param jobCheckInterval the interval with which the instance should check for new jobs to compute
     * @param heartbeatInterval the interval with which the instance should update its heartbeat and check for dead instances
     * @param oldJobDeletionInterval the interval with which the instance should check for old jobs to be deleted
     * @param deleteOldJobsAfter the time after which finished jobs should be deleted. Default is 365 days. Usually, this value can be set much lower (e.g.
     * to one day or even less). Note that, depending on the number of requests and the size of the input and result, the job database may become very large if
     * this value is set too high.
     * @param restartRunningJobsOnStartup whether we should check for running jobs of this instance at startup and restart them. The intention is that this
     * instance might have been restarted for whatever reason and still have the same name. If the instance was restarted when a job was running, this job would
     * still be in [JobStatus.RUNNING] state and nobody (including the instance itself) would recognize that the job is actually not computed anymore. So it is
     * highly recommended to set this property to `true`. *Note that this will block the instance on startup while the jobs are being reset, but this should
     * usually be a very short amount of time.*
     * @param threadPoolSize the number of threads used by the maintenance scheduler. All maintenance scheduling tasks are running in their own thread pool
     * to ensure that especially the update of heartbeats is not blocked by long-running computations.
     */
    @KJobsDsl
    class MaintenanceConfig internal constructor(
        var jobCheckInterval: Duration = 5.seconds,
        var heartbeatInterval: Duration = 10.seconds,
        var oldJobDeletionInterval: Duration = 1.days,
        var deleteOldJobsAfter: Duration = 365.days,
        var restartRunningJobsOnStartup: Boolean = true,
        var threadPoolSize: Int = 2,
    )

    /**
     * Configuration options for the cancellation of jobs.
     *
     * [checkInterval] is needed to update the set of jobs in status [JobStatus.CANCEL_REQUESTED] from the database. These are jobs which are
     * already running and will try to be aborted by cancelling their [coroutine job][kotlinx.coroutines.Job].
     *
     * @param enabled whether cancellation is enabled or not. This will enable the resource `POST cancel/{uuid}`. Default is `false`.
     * @param checkInterval the interval with which the instance should check for cancelled jobs. Default is 1 second. This value can be set much
     * higher depending on your needs (i.e. how urgent it is to abort a cancelled job which is still running).
     */
    @KJobsDsl
    class CancellationConfig internal constructor(
        var enabled: Boolean = false,
        var checkInterval: Duration = 1.seconds,
    )

    fun build() {
        if (maintenanceConfig.restartRunningJobsOnStartup) runBlocking {
            Maintenance.resetMyRunningJobs(jobPersistence, myInstanceName, persistencesPerType, restartsPerType)
        }
        apis.values.forEach { it.build(cancellationConfig.enabled) }
        if (maintenanceEnabled) {
            val executor = generateJobExecutor()
            val dispatcher = Executors.newFixedThreadPool(maintenanceConfig.threadPoolSize).asCoroutineDispatcher()
            dispatcher.scheduleForever(maintenanceConfig.jobCheckInterval, executorConfig.dispatcher) { executor.execute() }
            dispatcher.scheduleForever(maintenanceConfig.heartbeatInterval, Dispatchers.IO) { Maintenance.updateHeartbeat(jobPersistence, myInstanceName) }
            dispatcher.scheduleForever(maintenanceConfig.heartbeatInterval, Dispatchers.IO) {
                Maintenance.restartJobsFromDeadInstances(jobPersistence, persistencesPerType, maintenanceConfig.heartbeatInterval, restartsPerType)
            }
            dispatcher.scheduleForever(maintenanceConfig.oldJobDeletionInterval, Dispatchers.IO) {
                Maintenance.deleteOldJobs(jobPersistence, maintenanceConfig.deleteOldJobsAfter, persistencesPerType)
            }
            if (cancellationConfig.enabled) {
                dispatcher.scheduleForever(cancellationConfig.checkInterval, Dispatchers.IO) { Maintenance.checkForCancellations(jobPersistence) }
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    internal fun buildTestingMode(): JobFrameworkTestingApi {
        build()
        val jobConfigs = apis.mapValues {
            (it.value as ApiBuilder<Any, Any>).jobConfig.toJobConfig(it.key, myInstanceName, persistencesPerType[it.key]!! as DataPersistence<Any, Any>)
        } + jobs.mapValues {
            (it.value as JobBuilder<Any, Any>).jobConfig.toJobConfig(it.key, myInstanceName, persistencesPerType[it.key]!! as DataPersistence<Any, Any>)
        }
        return JobFrameworkTestingApi(
            jobPersistence, myInstanceName, jobConfigs, persistencesPerType, executorsPerType, executorConfig,
            maintenanceConfig, cancellationConfig, restartsPerType,
        )
    }

    private fun checkForDuplicateType(jobType: String) {
        require(!(jobType in apis || jobType in jobs || jobType in executorsPerType)) { "An API or job with type $jobType is already defined" }
    }

    private fun generateJobExecutor(): MainJobExecutor = MainJobExecutor(
        jobPersistence, myInstanceName, executorConfig.executionCapacityProvider,
        executorConfig.jobPrioritizer, executorConfig.tagMatcher, cancellationConfig, executorsPerType
    )
}

/**
 * Builder for an API.
 */
@KJobsDsl
open class ApiBuilder<INPUT, RESULT> internal constructor(
    protected val myInstanceName: String,
    private val jobType: String,
    private val route: Route,
    private val persistence: DataPersistence<INPUT, RESULT>,
    private val inputReceiver: suspend PipelineContext<Unit, ApplicationCall>.() -> INPUT,
    private val resultResponder: suspend PipelineContext<Unit, ApplicationCall>.(RESULT) -> Unit,
) {
    @Suppress("LateinitUsage") // this is an internal API and we (hopefully) know what we're doing
    internal lateinit var computation: suspend (Job, INPUT) -> ComputationResult<RESULT>
    private val apiConfig: ApiConfigBuilder<INPUT> = ApiConfigBuilder()
    internal val jobConfig: JobConfigBuilder<INPUT> = JobConfigBuilder()
    private val syncConfig: SynchronousResourceConfigBuilder<INPUT> = SynchronousResourceConfigBuilder()
    private val infoConfig: JobInfoConfigBuilder = JobInfoConfigBuilder()

    /**
     * Provides further configuration options about the API.
     */
    fun apiConfig(configuration: ApiConfigBuilder<INPUT>.() -> Unit) = configuration(apiConfig)

    /**
     * Provides further configuration options about the handling of jobs.
     */
    fun jobConfig(configuration: JobConfigBuilder<INPUT>.() -> Unit) = configuration(jobConfig)

    /**
     * Provides configuration options for the synchronous resource.
     */
    fun synchronousResourceConfig(configuration: SynchronousResourceConfigBuilder<INPUT>.() -> Unit) = configuration(syncConfig)

    /**
     * Provides configuration options for the job info resource.
     */
    fun infoConfig(configuration: JobInfoConfigBuilder.() -> Unit) = configuration(infoConfig)

    internal fun specificExecutor(): SpecificExecutor<INPUT, RESULT> =
        SpecificExecutor(myInstanceName, persistence, computation, jobConfig.timeoutComputation, jobConfig.maxRestarts)

    internal fun build(enableCancellation: Boolean) = with(route) {
        setupJobApi(
            apiConfig.toApiConfig(inputReceiver, resultResponder, enableCancellation, syncConfig.toSynchronousResourceConfig(), infoConfig.toJobInfoConfig()),
            jobConfig.toJobConfig(jobType, myInstanceName, persistence)
        )
    }
}

/**
 * Builder for job without a route.
 */
@KJobsDsl
class JobBuilder<INPUT, RESULT> internal constructor(
    private val myInstanceName: String,
    private val persistence: DataPersistence<INPUT, RESULT>,
    private val computation: suspend (Job, INPUT) -> ComputationResult<RESULT>,
) {
    internal val jobConfig: JobConfigBuilder<INPUT> = JobConfigBuilder()

    /**
     * Provides further configuration options about the handling of jobs.
     */
    fun jobConfig(configuration: JobConfigBuilder<INPUT>.() -> Unit) = configuration(jobConfig)

    internal fun specificExecutor(): SpecificExecutor<INPUT, RESULT> =
        SpecificExecutor(myInstanceName, persistence, computation, jobConfig.timeoutComputation, jobConfig.maxRestarts)
}

/**
 * Builder for a hierarchical API.
 */
@KJobsDsl
class HierarchicalApiBuilder<INPUT, RESULT> internal constructor(
    myInstanceName: String,
    jobType: String,
    route: Route,
    persistence: DataPersistence<INPUT, RESULT>,
    inputReceiver: suspend PipelineContext<Unit, ApplicationCall>.() -> INPUT,
    resultResponder: suspend PipelineContext<Unit, ApplicationCall>.(RESULT) -> Unit,
    internal val superComputation: suspend (Job, INPUT, Map<String, HierarchicalJobApi<*, *>>) -> ComputationResult<RESULT>,
) : ApiBuilder<INPUT, RESULT>(myInstanceName, jobType, route, persistence, inputReceiver, resultResponder) {

    internal val dependents: MutableMap<String, Triple<JobConfig<*, *>, SpecificExecutor<*, *>, DataPersistence<*, *>>> = mutableMapOf()

    /**
     * Adds a new dependent job with the given parameters.
     * @param jobType a unique name identifying this type of job
     * @param persistence the persistence/database to use for storing the inputs and results for this dependent job
     * @param computation the actual computation
     * @param configuration provides more detailed configuration options as described in [ApiBuilder.JobConfigBuilder]
     */
    fun <DEP_INPUT, DEP_RESULT> addDependentJob(
        jobType: String,
        persistence: DataPersistence<DEP_INPUT, DEP_RESULT>,
        computation: suspend (Job, DEP_INPUT) -> ComputationResult<DEP_RESULT>,
        configuration: JobConfigBuilder<DEP_INPUT>.() -> Unit
    ) {
        JobConfigBuilder<DEP_INPUT>().apply(configuration).let { config ->
            require(jobType !in dependents) { "A dependent job with type $jobType is already defined" }
            dependents[jobType] = Triple(
                JobConfig(jobType, persistence, myInstanceName, config.tagProvider, config.customInfoProvider, config.priorityProvider),
                SpecificExecutor(myInstanceName, persistence, computation, config.timeoutComputation, config.maxRestarts),
                persistence
            )
        }
    }
}

/**
 * Further configuration options for the API.
 * @param basePath an additional base path of the application (in addition to what is effectively predefined by the [Route] passed into
 * [JobFrameworkBuilder.addApi]). Default is the empty string.
 * @param inputValidation an optional validation of the input which is performed in the `submit` resource. Must return a list of error messages which
 * is empty in case the validation did not find any errors. If the list is not empty, the request is rejected with [HttpStatusCode.NotFound] and
 * a message constructed from the list. Default is an empty list.
 * @param enableDeletion whether a `DELETE` resource should be added which allows the API user to delete a job (usually once the result has been fetched)
 */
@KJobsDsl
class ApiConfigBuilder<INPUT> internal constructor(
    var basePath: String? = null,
    var inputValidation: (INPUT) -> List<String> = { emptyList() },
    var enableDeletion: Boolean = false,
) {
    internal fun <RESULT> toApiConfig(
        inputReceiver: suspend PipelineContext<Unit, ApplicationCall>.() -> INPUT,
        resultResponder: suspend PipelineContext<Unit, ApplicationCall>.(RESULT) -> Unit,
        enableCancellation: Boolean,
        syncMockConfig: SynchronousResourceConfig<INPUT>,
        jobInfoConfig: JobInfoConfig
    ) = ApiConfig(inputReceiver, resultResponder, basePath, inputValidation, enableDeletion, enableCancellation, syncMockConfig, jobInfoConfig)
}

/**
 * Further configuration options about the handling of jobs.
 * @param tagProvider a function providing a list of tags (strings) for a job input. These tags are stored in [Job.tags]. Default is an empty list.
 * @param customInfoProvider a function providing a list of tags (strings) for a job input. These tags are stored in [Job.customInfo].
 * Default is a function returning `null`.
 * @param priorityProvider a function providing an integer priority for a job input. A smaller number means a higher priority. Default is 0.
 * @param timeoutComputation a function providing a timeout for the given job. The default is 24 hours. In most cases this default should be set much lower.
 * @param maxRestarts the maximum number of restarts for this job in case of (potentially temporary) errors. Default is [DEFAULT_MAX_JOB_RESTARTS].
 */
@KJobsDsl
class JobConfigBuilder<INPUT> internal constructor(
    var tagProvider: (INPUT) -> List<String> = { emptyList() },
    var customInfoProvider: (INPUT) -> String? = { null },
    var priorityProvider: (INPUT) -> Int = { 0 },
    var timeoutComputation: (Job, INPUT) -> Duration = { _, _ -> 24.hours },
    var maxRestarts: Int = DEFAULT_MAX_JOB_RESTARTS,
) {
    internal fun <RESULT> toJobConfig(jobType: String, myInstanceName: String, persistence: DataPersistence<INPUT, RESULT>) =
        JobConfig(jobType, persistence, myInstanceName, tagProvider, customInfoProvider, priorityProvider)
}

/**
 * Configuration options for the synchronous resource.
 *
 * The synchronous resource is a wrapper around the asynchronous resources (`submit`, `status`, `result`, etc). It does not perform the computation itself,
 * but (like the asynchronous API) stores the job in the database and then waits for the job to be computed.
 *
 * To accelerate the selection of such jobs you might want to configure a [custom priority provider][customPriorityProvider].
 *
 * Note that even with a small [checkInterval] and if there are no other jobs running, it might take up to the duration specified in
 * [JobFrameworkBuilder.MaintenanceConfig.jobCheckInterval] until the computation of the job actually starts.
 *
 * By default, the synchronous API is disabled.
 * @param enabled whether the synchronous resource is enabled for this API, by default it is not enabled
 * @param path the path on which the synchronous resource should be placed, default is `synchronous`
 * @param checkInterval the interval in which the job status should be checked after it was submitted
 * @param maxWaitingTime the maximum time to wait for the job to complete. If this time is exceeded, status 400 is returned including the information
 * about the UUID of the generated job.
 * @param customPriorityProvider a custom priority provider for jobs from the synchronous resource
 */
@KJobsDsl
class SynchronousResourceConfigBuilder<INPUT>(
    var enabled: Boolean = false,
    var path: String = "synchronous",
    var checkInterval: Duration = 200.milliseconds,
    var maxWaitingTime: Duration = 1.hours,
    var customPriorityProvider: (INPUT) -> Int = { 0 }
) {
    internal fun toSynchronousResourceConfig() = SynchronousResourceConfig(enabled, path, checkInterval, maxWaitingTime, customPriorityProvider)
}

/**
 * Configuration for the job info.
 *
 * If [enabled] the KJobs will provide an additional `GET` resource on the given [path] which can return general information
 * about the job. The content returned by the resource can be configured via the [responder].
 *
 * Note that the default implementation of the [responder] requires JSON serialization to be installed in the Ktor server.
 *
 * @param enabled whether the job info resource is enabled or not, by default it is not enabled
 * @param path the path on which the job info should be placed, default is `info`
 * @param responder describes how the job info should be returned, by default the [Job] is serialized and returned as JSON
 */
@KJobsDsl
class JobInfoConfigBuilder(
    var enabled: Boolean = false,
    var path: String = "info",
    var responder: suspend PipelineContext<Unit, ApplicationCall>.(Job) -> Unit = { call.respond(it) }
) {
    internal fun toJobInfoConfig() = JobInfoConfig(enabled, path, responder)
}

internal const val DEFAULT_MAX_JOB_RESTARTS = 3
