// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.api

import com.booleworks.kjobs.api.JobFrameworkBuilder.MaintenanceConfig
import com.booleworks.kjobs.common.Either
import com.booleworks.kjobs.control.MainJobExecutor
import com.booleworks.kjobs.control.Maintenance
import com.booleworks.kjobs.control.SpecificExecutor
import com.booleworks.kjobs.control.submit
import com.booleworks.kjobs.data.ExecutionCapacityProvider
import com.booleworks.kjobs.data.JobPrioritizer
import com.booleworks.kjobs.data.PersistenceAccessResult
import com.booleworks.kjobs.data.TagMatcher
import io.ktor.server.application.Application
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking
import kotlin.time.Duration

/**
 * Set up the job framework in testing mode. This returns a [JobFrameworkTestingMode] object which allows
 * to manually run maintenance jobs or executors.
 * @param myInstanceName a unique identifier of this instance, e.g. in a Kubernetes environment.
 * Can be an arbitrary, non-empty, string if there is only a single instance.
 * @param jobPersistence an object allowing to store and retrieve jobs (and heartbeats) from some
 * (usually external) data source, e.g. a redis cache or a postgres database
 * @param executionEnvironment either a specific [CoroutineScope] or a ktor [Application] in which
 * maintenance and execution jobs are launched
 * @param maintenanceEnabled whether all kinds of background jobs (including the [MainJobExecutor])
 * should be run according to [MaintenanceConfig] or not. This may be useful is you want jobs only be
 * triggered via the returned [JobFrameworkTestingMode] object.
 * @param configuration additional configuration options, in particular this includes the
 * possibility to [add APIs][JobFrameworkBuilder.addApi] to a ktor application
 */
@Suppress("FunctionName")
fun JobFrameworkTestingMode(
    myInstanceName: String,
    jobPersistence: JobPersistence,
    executionEnvironment: Either<CoroutineScope, Application>,
    maintenanceEnabled: Boolean,
    configuration: JobFrameworkBuilder.() -> Unit
) = JobFrameworkBuilder(myInstanceName, jobPersistence, executionEnvironment, maintenanceEnabled).apply(configuration).buildTestingMode()

/**
 * A class providing testing access for some features of the job framework.
 *
 * It allows to run the executor or other [Maintenance] jobs, and to submit jobs or fetch results.
 */
class JobFrameworkTestingApi internal constructor(
    private val jobPersistence: JobPersistence,
    private val myInstanceName: String,
    private val persistencesPerType: Map<String, DataPersistence<*, *>>,
    private val executorsPerType: Map<String, SpecificExecutor<*, *>>,
    private val executionCapacityProvider: ExecutionCapacityProvider,
    private val jobPrioritizer: JobPrioritizer,
    private val tagMatcher: TagMatcher,
    private val heartbeatInterval: Duration,
    private val deleteOldJobsAfter: Duration,
    private val maxRestarts: Int,
) {
    /**
     * Runs the executor *once* with the given optional parameters. If the parameters are not given,
     * the values provided in [JobFrameworkTestingMode] are used.
     */
    fun runExecutor(
        executionCapacityProvider: ExecutionCapacityProvider = this.executionCapacityProvider,
        jobPrioritizer: JobPrioritizer = this.jobPrioritizer,
        tagMatcher: TagMatcher = this.tagMatcher
    ) = runBlocking { MainJobExecutor(jobPersistence, myInstanceName, executionCapacityProvider, jobPrioritizer, tagMatcher, executorsPerType).execute() }

    /**
     * Submits a new job.
     */
    fun <INPUT> submitJob(
        jobType: String,
        input: INPUT,
        instance: String = myInstanceName,
        tagProvider: (INPUT) -> List<String> = { emptyList() },
        customInfoProvider: (INPUT) -> String = { "" },
        priorityProvider: (INPUT) -> Int = { 0 }
    ): PersistenceAccessResult<String> = runBlocking {
        @Suppress("UNCHECKED_CAST")
        submit(jobType, input, instance, persistencesPerType[jobType] as DataPersistence<INPUT, *>, tagProvider, customInfoProvider, priorityProvider)
    }

    /**
     * Updates the heartbeat of the given [instance], the default is the instance provided in [JobFrameworkTestingMode].
     * @see Maintenance.updateHeartbeat
     */
    fun updateHeartbeat(instance: String = myInstanceName) = runBlocking { Maintenance.updateHeartbeat(jobPersistence, instance) }

    /**
     * Performs the cancellation check which updates the internal list of jobs to be cancelled.
     * @see Maintenance.checkForCancellations
     */
    fun checkForCancellations() = runBlocking { Maintenance.checkForCancellations(jobPersistence) }

    /**
     * Restarts jobs from dead instances.
     * @see Maintenance.restartJobsFromDeadInstances
     */
    fun restartJobsFromDeadInstances() =
        runBlocking { Maintenance.restartJobsFromDeadInstances(jobPersistence, persistencesPerType, heartbeatInterval, maxRestarts) }

    /**
     * Deletes old jobs.
     * @see Maintenance.deleteOldJobs
     */
    fun deleteOldJobs() = runBlocking { Maintenance.deleteOldJobs(jobPersistence, deleteOldJobsAfter, persistencesPerType) }

    /**
     * Resets running jobs of the given [instance], the default is the instance provided in [JobFrameworkTestingMode].
     * @see Maintenance.resetMyRunningJobs
     */
    fun resetMyRunningJobs(instance: String = myInstanceName) =
        runBlocking { Maintenance.resetMyRunningJobs(jobPersistence, instance, persistencesPerType, maxRestarts) }

    /**
     * Schedules the job with the given [uuid] for cancellation.
     */
    fun cancelJob(uuid: String) {
        Maintenance.jobsToBeCancelled += uuid
    }
}
