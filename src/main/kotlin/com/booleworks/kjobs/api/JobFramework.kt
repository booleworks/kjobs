// SPDX-License-Identifier: MIT
// Copyright 2023-2024 BooleWorks GmbH

@file:Suppress("LateinitUsage") // this is an internal API and we (hopefully) know what we're doing

package com.booleworks.kjobs.api

import com.booleworks.kjobs.api.hierarchical.HierarchicalJobApi
import com.booleworks.kjobs.api.hierarchical.HierarchicalJobApiImpl
import com.booleworks.kjobs.api.persistence.DataPersistence
import com.booleworks.kjobs.api.persistence.JobPersistence
import com.booleworks.kjobs.control.ComputationResult
import com.booleworks.kjobs.control.MainJobExecutor
import com.booleworks.kjobs.control.Maintenance
import com.booleworks.kjobs.control.SpecificExecutor
import com.booleworks.kjobs.control.polling.LongPollManager
import com.booleworks.kjobs.control.polling.NopLongPollManager
import com.booleworks.kjobs.control.scheduleForever
import com.booleworks.kjobs.control.setupJobApi
import com.booleworks.kjobs.data.ApiConfig
import com.booleworks.kjobs.data.DefaultExecutionCapacityProvider
import com.booleworks.kjobs.data.DefaultJobPrioritizer
import com.booleworks.kjobs.data.ExecutionCapacityProvider
import com.booleworks.kjobs.data.Job
import com.booleworks.kjobs.data.JobConfig
import com.booleworks.kjobs.data.JobInfoConfig
import com.booleworks.kjobs.data.JobPrioritizer
import com.booleworks.kjobs.data.JobStatistics
import com.booleworks.kjobs.data.JobStatus
import com.booleworks.kjobs.data.LongPollingConfig
import com.booleworks.kjobs.data.PollStatus
import com.booleworks.kjobs.data.SynchronousResourceConfig
import com.booleworks.kjobs.data.TagMatcher
import io.ktor.http.HttpStatusCode
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.server.routing.RoutingContext
import io.ktor.server.routing.delete
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
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
 * @return a [coroutine job][kotlinx.coroutines.Job] which can be used to cancel all maintenance
 * jobs started by the KJobs. This is only relevant if you want to stop the Job Framework without
 * terminating the JVM to do something different with it.
 */
@Suppress("FunctionName")
@KJobsDsl
fun JobFramework(
    myInstanceName: String,
    jobPersistence: JobPersistence,
    configuration: JobFrameworkBuilder.() -> Unit
): kotlinx.coroutines.Job? = JobFrameworkBuilder(myInstanceName, jobPersistence).apply(configuration).build()

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
    private val statisticsConfig: StatisticsConfig = StatisticsConfig(jobPersistence)
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
        inputReceiver: suspend RoutingContext.() -> INPUT,
        resultResponder: suspend RoutingContext.(RESULT) -> Unit,
        computation: suspend (Job, INPUT) -> ComputationResult<RESULT>,
        configuration: ApiBuilder<INPUT, RESULT>.() -> Unit = {}
    ): ApiBuilder<INPUT, RESULT> {
        checkForDuplicateType(jobType)
        return ApiBuilder(myInstanceName, jobType, route, dataPersistence, inputReceiver, resultResponder).apply {
            configuration()
            this.computation = computation
            this@JobFrameworkBuilder.apis[jobType] = this
            this@JobFrameworkBuilder.executorsPerType[jobType] = specificExecutor(longPollingConfig.longPollManager)
            this@JobFrameworkBuilder.persistencesPerType[jobType] = dataPersistence
            this@JobFrameworkBuilder.restartsPerType[jobType] = jobConfig.maxRestarts
        }
    }

    /**
     * Generates a new API for a hierarchical job.
     *
     * **This is an experimental API which may change in the future without prior notice! It should also be noted
     * that especially the implementation of the [parentComputation] (which has to be provided by the library
     * user) is not trivial.**
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
        inputReceiver: suspend RoutingContext.() -> INPUT,
        resultResponder: suspend RoutingContext.(RESULT) -> Unit,
        parentComputation: suspend (Job, INPUT, Map<String, HierarchicalJobApi<*, *>>) -> ComputationResult<RESULT>,
        configuration: HierarchicalApiBuilder<INPUT, RESULT>.() -> Unit = {}
    ): HierarchicalApiBuilder<INPUT, RESULT> {
        checkForDuplicateType(jobType)
        return HierarchicalApiBuilder(myInstanceName, jobType, route, dataPersistence, inputReceiver, resultResponder, parentComputation).apply {
            configuration()
            dependents.keys.forEach { this@JobFrameworkBuilder.checkForDuplicateType(it) }
            this.computation = { job, input -> superComputation(job, input, dependents.mapValues { HierarchicalJobApiImpl(it.value.first, job.uuid) }) }
            this@JobFrameworkBuilder.executorsPerType[jobType] = specificExecutor(longPollingConfig.longPollManager)
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
     * Enables the cancellation of jobs.
     */
    fun enableCancellation(configuration: CancellationConfig.() -> Unit = {}) {
        cancellationConfig.enabled = true
        configuration(cancellationConfig)
    }

    /**
     * Enables the global statistics resource.
     * @param basePath the path on which the statistics resource is enabled
     */
    fun enableStatistics(basePath: Route, configuration: StatisticsConfig.() -> Unit = {}) {
        statisticsConfig.enabled = true
        statisticsConfig.basePath = basePath
        configuration(statisticsConfig)
    }

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
     * @param jobCheckInterval the interval with which the instance should check for new jobs to compute. *If the check takes longer than the given interval,
     * the next execution will be delayed until the previous check has finished.* So there will not be multiple job checks be executed in parallel on the same
     * instance (because it may e.g. lead to inconsistencies if the same job is reserved twice by the same instance).
     * @param heartbeatTimeout the interval since the latest heartbeat after which the instance is considered to be broken or dead. This interval should be
     * (significantly) longer than the [jobCheckInterval] to ensure that there is enough time for the heartbeat to be updated.
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
        var heartbeatTimeout: Duration = 1.minutes,
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
     * @param checkInterval the interval with which the instance should check for cancelled jobs. Default is 1 second. This value can be set much
     * higher depending on your needs (i.e. how urgent it is to abort a cancelled job which is still running).
     */
    @KJobsDsl
    class CancellationConfig internal constructor(
        internal var enabled: Boolean = false,
        var checkInterval: Duration = 1.seconds,
    )

    /**
     * Configuration options for a global statistics resource.
     *
     * This resource is intended to provide general information about the jobs which are currently stored in the database.
     *
     * [statisticsGenerator] is a function providing the statistics object. You can let it create your own arbitrary object or use one of the provided functions
     * [JobStatistics.forAllJobs] (which is the default) or [JobStatistics.byType].
     *
     * [routeDefinition] allows to override the generated route (including its definition and what it returns). By default, a resource `GET /statistics`
     * is generated which returns the result of the [statisticsGenerator].
     */
    @KJobsDsl
    class StatisticsConfig internal constructor(persistence: JobPersistence) {
        internal var enabled: Boolean = false
        internal lateinit var basePath: Route

        private var _statisticsGenerator: AtomicReference<suspend () -> Any> = AtomicReference(suspend { JobStatistics.forAllJobs(persistence) })
        var statisticsGenerator: suspend () -> Any
            get() = _statisticsGenerator.get()
            set(value) = _statisticsGenerator.set(value)

        var routeDefinition: Route.() -> Unit = { get("statistics") { call.respond(statisticsGenerator()) } }
    }

    internal fun build(): kotlinx.coroutines.Job? {
        if (maintenanceConfig.restartRunningJobsOnStartup) runBlocking {
            Maintenance.resetMyRunningJobs(jobPersistence, myInstanceName, persistencesPerType, restartsPerType)
        }
        apis.values.forEach { it.build(cancellationConfig.enabled) }
        if (statisticsConfig.enabled) {
            with(statisticsConfig) {
                basePath.routeDefinition()
            }
        }
        return if (maintenanceEnabled) {
            val supervisor = SupervisorJob()
            val jobCancellationQueue = AtomicReference(setOf<String>())
            val executor = generateJobExecutor(jobCancellationQueue)
            val dispatcher = Executors.newFixedThreadPool(maintenanceConfig.threadPoolSize).asCoroutineDispatcher() + supervisor
            dispatcher.scheduleForever(maintenanceConfig.jobCheckInterval, "Main executor run", true, executorConfig.dispatcher) {
                executor.execute()
            }
            dispatcher.scheduleForever(maintenanceConfig.heartbeatTimeout, "Restart jobs of dead instances", true, Dispatchers.IO) {
                Maintenance.restartJobsFromDeadInstances(jobPersistence, persistencesPerType, maintenanceConfig.heartbeatTimeout, restartsPerType)
            }
            dispatcher.scheduleForever(maintenanceConfig.oldJobDeletionInterval, "Delete old jobs", true, Dispatchers.IO) {
                Maintenance.deleteOldJobs(jobPersistence, maintenanceConfig.deleteOldJobsAfter, persistencesPerType)
            }
            if (cancellationConfig.enabled) {
                dispatcher.scheduleForever(cancellationConfig.checkInterval, "Check for cancellations", true, Dispatchers.IO) {
                    Maintenance.checkForCancellations(jobPersistence, jobCancellationQueue)
                }
            }
            return supervisor
        } else null
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

    private fun generateJobExecutor(jobCancellationQueue: AtomicReference<Set<String>>): MainJobExecutor = MainJobExecutor(
        jobPersistence, myInstanceName, executorConfig.executionCapacityProvider, executorConfig.jobPrioritizer,
        executorConfig.tagMatcher, cancellationConfig, jobCancellationQueue, executorsPerType
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
    private val inputReceiver: suspend RoutingContext.() -> INPUT,
    private val resultResponder: suspend RoutingContext.(RESULT) -> Unit,
) {
    internal lateinit var computation: suspend (Job, INPUT) -> ComputationResult<RESULT>
    private val apiConfig: ApiConfigBuilder<INPUT> = ApiConfigBuilder()
    internal val jobConfig: JobConfigBuilder<INPUT> = JobConfigBuilder()
    private val syncConfig: SynchronousResourceConfigBuilder<INPUT> = SynchronousResourceConfigBuilder()
    private val infoConfig: JobInfoConfigBuilder = JobInfoConfigBuilder()
    internal val longPollingConfig: LongPollingConfigBuilder = LongPollingConfigBuilder()

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
    fun enableSynchronousResource(configuration: SynchronousResourceConfigBuilder<INPUT>.() -> Unit = {}) {
        syncConfig.enabled = true
        configuration(syncConfig)
    }

    /**
     * Provides configuration options for the job info resource.
     */
    fun enableJobInfoResource(configuration: JobInfoConfigBuilder.() -> Unit = {}) {
        infoConfig.enabled = true
        configuration(infoConfig)
    }

    /**
     * Provides configuration options for the long polling resource.
     */
    fun enableLongPolling(longPollManager: () -> LongPollManager, configuration: LongPollingConfigBuilder.() -> Unit = {}) {
        longPollingConfig.enabled = true
        longPollingConfig.longPollManager = longPollManager
        configuration(longPollingConfig)
    }

    internal fun specificExecutor(longPollManager: () -> LongPollManager): SpecificExecutor<INPUT, RESULT> =
        SpecificExecutor(myInstanceName, persistence, computation, longPollManager, jobConfig.timeoutComputation, jobConfig.maxRestarts)

    internal fun build(enableCancellation: Boolean) = with(route) {
        setupJobApi(
            apiConfig.toApiConfig(
                inputReceiver,
                resultResponder,
                enableCancellation,
                syncConfig.toSynchronousResourceConfig(),
                infoConfig.toJobInfoConfig(),
                longPollingConfig.toLongPollingConfig()
            ),
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
        SpecificExecutor(myInstanceName, persistence, computation, { NopLongPollManager }, jobConfig.timeoutComputation, jobConfig.maxRestarts)
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
    inputReceiver: suspend RoutingContext.() -> INPUT,
    resultResponder: suspend RoutingContext.(RESULT) -> Unit,
    internal val superComputation: suspend (Job, INPUT, Map<String, HierarchicalJobApi<*, *>>) -> ComputationResult<RESULT>,
) : ApiBuilder<INPUT, RESULT>(myInstanceName, jobType, route, persistence, inputReceiver, resultResponder) {

    internal val dependents: MutableMap<String, Triple<JobConfig<*, *>, SpecificExecutor<*, *>, DataPersistence<*, *>>> = mutableMapOf()

    /**
     * Adds a new dependent job with the given parameters.
     * @param jobType a unique name identifying this type of job
     * @param persistence the persistence/database to use for storing the inputs and results for this dependent job
     * @param computation the actual computation
     * @param configuration provides more detailed configuration options as described in [JobConfigBuilder]
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
                SpecificExecutor(myInstanceName, persistence, computation, { NopLongPollManager }, config.timeoutComputation, config.maxRestarts),
                persistence
            )
        }
    }
}

/**
 * Further configuration options for the API.
 *
 * The `*Route` parameters allow to reconfigure the command to generate the route. There are different purposes to reconfigure such commands:
 *
 * - adjust the path on which the route is hosted (e.g. `POST submit-my-job` instead of `POST submit`)
 * - use a different HTTP method (e.g. if you prefer `POST` oder `DELETE` in the `deleteRoute`)
 * - **use an entirely different route-creation command** which can be useful to integrate some frameworks which generate OpenAPI definitions based on
 * rewritten `route` commands
 *
 * **Note that all routes except for `submitRoute` and `syncRoute` must provide the `uuid` as path parameter!**
 *
 * @param inputValidation an optional validation of the input which is performed in the `submit` resource. Must return a list of error messages which
 * is empty in case the validation did not find any errors. If the list is not empty, the request is rejected with [HttpStatusCode.BadRequest] and
 * a message constructed from the list. Default is an empty list.
 * @param enableDeletion whether a `DELETE` resource should be added which allows the API user to delete a job (usually once the result has been fetched)
 * @param submitRoute replacement for the submit resource, default is `POST submit`
 * @param statusRoute replacement for the status resource, default is `GET status/{uuid}`
 * @param resultRoute replacement for the result resource, default is `GET result/{uuid}`
 * @param failureRoute replacement for the failure resource, default is `GET failure/{uuid}`
 * @param deleteRoute replacement for the delete resource, default is `DELETE delete/{uuid}`, only used if the resource is enabled
 * @param cancelRoute replacement for the cancel resource, default is `POST cancel/{uuid}`, only used if the resource is enabled
 * @param syncRoute replacement for the sync resource, default is `POST sync`, only used if the resource is enabled
 * @param infoRoute replacement for the info resource, default is `GET info/{uuid}`, only used if the resource is enabled
 * @param longPollingRoute replacement for the long polling resource, default is `GET poll/{uuid}`, only used if the resource is enabled
 */
@KJobsDsl
class ApiConfigBuilder<INPUT> internal constructor(
    var inputValidation: (INPUT) -> InputValidationResult = { InputValidationResult.success() },
    var enableDeletion: Boolean = false,
    var submitRoute: Route.(suspend RoutingContext.() -> Unit) -> Unit = { block -> post("submit") { block() } },
    var statusRoute: Route.(suspend RoutingContext.() -> Unit) -> Unit = { block -> get("status/{uuid}") { block() } },
    var resultRoute: Route.(suspend RoutingContext.() -> Unit) -> Unit = { block -> get("result/{uuid}") { block() } },
    var failureRoute: Route.(suspend RoutingContext.() -> Unit) -> Unit = { block -> get("failure/{uuid}") { block() } },
    var deleteRoute: Route.(suspend RoutingContext.() -> Unit) -> Unit = { block -> delete("delete/{uuid}") { block() } },
    var cancelRoute: Route.(suspend RoutingContext.() -> Unit) -> Unit = { block -> post("cancel/{uuid}") { block() } },
    var syncRoute: Route.(suspend RoutingContext.() -> Unit) -> Unit = { block -> post("synchronous") { block() } },
    var infoRoute: Route.(suspend RoutingContext.() -> Unit) -> Unit = { block -> get("info/{uuid}") { block() } },
    var longPollingRoute: Route.(suspend RoutingContext.() -> Unit) -> Unit = { block -> get("poll/{uuid}") { block() } },
) {
    internal fun <RESULT> toApiConfig(
        inputReceiver: suspend RoutingContext.() -> INPUT,
        resultResponder: suspend RoutingContext.(RESULT) -> Unit,
        enableCancellation: Boolean,
        syncMockConfig: SynchronousResourceConfig<INPUT>,
        jobInfoConfig: JobInfoConfig,
        longPollingConfig: LongPollingConfig
    ) = ApiConfig(
        inputReceiver, resultResponder, inputValidation, enableDeletion, enableCancellation, syncMockConfig, jobInfoConfig, longPollingConfig,
        submitRoute, statusRoute, resultRoute, failureRoute, deleteRoute, cancelRoute, syncRoute, infoRoute, longPollingRoute
    )
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
 * @param checkInterval the interval in which the job status should be checked after it was submitted
 * @param maxWaitingTime the maximum time to wait for the job to complete. If this time is exceeded, status 400 is returned including the information
 * about the UUID of the generated job.
 * @param customPriorityProvider a custom priority provider for jobs from the synchronous resource
 */
@KJobsDsl
class SynchronousResourceConfigBuilder<INPUT>(
    internal var enabled: Boolean = false,
    var checkInterval: Duration = 200.milliseconds,
    var maxWaitingTime: Duration = 1.hours,
    var customPriorityProvider: (INPUT) -> Int = { 0 }
) {
    internal fun toSynchronousResourceConfig() = SynchronousResourceConfig(enabled, checkInterval, maxWaitingTime, customPriorityProvider)
}

/**
 * Configuration for the job info.
 *
 * If [enabled] the KJobs will provide an additional resource (defined in [ApiConfigBuilder.infoRoute], default `GET info/{uuid}`
 * which can return general information about the job. The content returned by the resource can be configured via the [responder].
 *
 * Note that the default implementation of the [responder] requires JSON serialization to be installed in the Ktor server.
 *
 * @param enabled whether the job info resource is enabled or not, by default it is not enabled
 * @param responder describes how the job info should be returned, by default the [Job] is serialized and returned as JSON
 */
@KJobsDsl
class JobInfoConfigBuilder(
    internal var enabled: Boolean = false,
    var responder: suspend RoutingContext.(Job) -> Unit = { call.respond(it) }
) {
    internal fun toJobInfoConfig() = JobInfoConfig(enabled, responder)
}

/**
 * Configuration for the long polling.
 *
 * If [enabled] the KJobs will provide an additional resource (defined in [ApiConfigBuilder.longPollingRoute], default `GET poll/{uuid}`)
 * which waits at most [maximumConnectionTimeout] for the result of the job with the given UUID. As soon as the result is available, the new
 * status of the job will be returned. If it is not available after the given connection timeout [PollStatus.TIMEOUT] is returned.
 *
 * The user may choose to use a smaller connection timeout in the request.
 *
 * @param enabled whether the long polling resource is enabled or not, by default it is not enabled
 * @param longPollManager the [LongPollManager] to be used for this API
 * @param maximumConnectionTimeout the maximum connection timeout after which [PollStatus.TIMEOUT] is returned
 */
@KJobsDsl
class LongPollingConfigBuilder(
    internal var enabled: Boolean = false,
    internal var longPollManager: () -> LongPollManager = { NopLongPollManager },
    var maximumConnectionTimeout: Duration = 3.minutes
) {
    internal fun toLongPollingConfig() = LongPollingConfig(enabled, longPollManager, maximumConnectionTimeout)
}

/**
 * Result of the input validation.
 *
 * @param success whether the validation of the input was successful or not. In case of `false`, no job will be created
 * for the input and the response code (usually 400, but could also be something more specific) and message should give
 * a hint about what went wrong. In case of `true`, status 200 will be returned without any messages.
 * @param message the message to be returned in case of an unsuccessful validation
 * @param responseCode the response code in case of an unsuccessful validation
 */
class InputValidationResult private constructor(
    val success: Boolean,
    val message: String,
    val responseCode: HttpStatusCode,
) {
    companion object {
        /**
         * Creates a new input validation result for a successful validation.
         */
        fun success() = InputValidationResult(true, "", HttpStatusCode.OK)

        /**
         * Creates a new input validation result for an unsuccessful validation with the given messages and optional response code.
         * @param message the error message to be returned in the response
         * @param responseCode the response code to return
         */
        fun failure(message: String, responseCode: HttpStatusCode = HttpStatusCode.BadRequest) = InputValidationResult(false, message, responseCode)
    }
}

internal const val DEFAULT_MAX_JOB_RESTARTS = 3
