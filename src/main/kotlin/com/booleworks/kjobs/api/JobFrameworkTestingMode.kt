// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.api

import com.booleworks.kjobs.api.JobFrameworkBuilder.MaintenanceConfig
import com.booleworks.kjobs.api.persistence.DataPersistence
import com.booleworks.kjobs.api.persistence.JobPersistence
import com.booleworks.kjobs.control.MainJobExecutor
import com.booleworks.kjobs.control.Maintenance
import com.booleworks.kjobs.control.SpecificExecutor
import com.booleworks.kjobs.control.submit
import com.booleworks.kjobs.data.ExecutionCapacityProvider
import com.booleworks.kjobs.data.Job
import com.booleworks.kjobs.data.JobConfig
import com.booleworks.kjobs.data.JobPrioritizer
import com.booleworks.kjobs.data.PersistenceAccessResult
import com.booleworks.kjobs.data.TagMatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicReference

/**
 * Set up the job framework in testing mode. This returns a [JobFrameworkTestingMode] object which allows
 * to manually run maintenance jobs or executors.
 * @param myInstanceName a unique identifier of this instance, e.g. in a Kubernetes environment.
 * Can be an arbitrary, non-empty, string if there is only a single instance.
 * @param jobPersistence an object allowing to store and retrieve jobs (and heartbeats) from some
 * (usually external) data source, e.g. a redis cache or a postgres database
 * @param maintenanceEnabled whether all kinds of background jobs (including the [MainJobExecutor])
 * should be run according to [MaintenanceConfig] or not. This may be useful is you want jobs only be
 * triggered via the returned [JobFrameworkTestingMode] object.
 * @param configuration additional configuration options, in particular this includes the
 * possibility to [add APIs][JobFrameworkBuilder.addApi] to a ktor application
 */
@Suppress("FunctionName")
@KJobsDsl
fun JobFrameworkTestingMode(
    myInstanceName: String,
    jobPersistence: JobPersistence,
    maintenanceEnabled: Boolean,
    configuration: JobFrameworkBuilder.() -> Unit
) = JobFrameworkBuilder(myInstanceName, jobPersistence, maintenanceEnabled).apply(configuration).buildTestingMode()

/**
 * A class providing testing access for some features of the job framework.
 *
 * It allows to run the executor or other [Maintenance] jobs, and to submit jobs or fetch results.
 */
@KJobsDsl
class JobFrameworkTestingApi internal constructor(
    private val jobPersistence: JobPersistence,
    private val myInstanceName: String,
    private val jobConfigs: Map<String, JobConfig<*, *>>,
    private val persistencesPerType: Map<String, DataPersistence<*, *>>,
    private val executorsPerType: Map<String, SpecificExecutor<*, *>>,
    private val executorConfig: JobFrameworkBuilder.ExecutorConfig,
    private val maintenanceConfig: MaintenanceConfig,
    private val cancellationConfig: JobFrameworkBuilder.CancellationConfig,
    private val maxRestartsPerType: Map<String, Int>,
) {
    /**
     * Runs the executor *once* with the given optional parameters. This function blocks until the
     * computation has finished.
     *
     * If the parameters are not given, the values provided in [JobFrameworkTestingMode] are used.
     */
    fun runExecutor(
        executionCapacityProvider: ExecutionCapacityProvider = executorConfig.executionCapacityProvider,
        jobPrioritizer: JobPrioritizer = executorConfig.jobPrioritizer,
        tagMatcher: TagMatcher = executorConfig.tagMatcher,
        jobCancellationQueue: AtomicReference<Set<String>> = AtomicReference(setOf())
    ) = runBlocking(Dispatchers.Default) {
        MainJobExecutor(
            jobPersistence,
            myInstanceName,
            executionCapacityProvider,
            jobPrioritizer,
            tagMatcher,
            cancellationConfig,
            jobCancellationQueue,
            executorsPerType
        ).execute()
    }

    /**
     * Submits a new job.
     */
    @Suppress("UNCHECKED_CAST")
    fun <INPUT> submitJob(
        jobType: String,
        input: INPUT,
        instance: String = myInstanceName,
        tagProvider: (INPUT) -> List<String> = (jobConfigs[jobType]!! as JobConfig<INPUT, *>).tagProvider,
        customInfoProvider: (INPUT) -> String? = (jobConfigs[jobType]!! as JobConfig<INPUT, *>).customInfoProvider,
        priorityProvider: (INPUT) -> Int = (jobConfigs[jobType]!! as JobConfig<INPUT, *>).priorityProvider
    ): PersistenceAccessResult<Job> = runBlocking {
        submit(
            input, JobConfig(jobType, persistencesPerType[jobType] as DataPersistence<INPUT, *>, instance, tagProvider, customInfoProvider, priorityProvider)
        )
    }

    /**
     * Updates the heartbeat of the given [instance], the default is the instance provided in [JobFrameworkTestingMode].
     * @see Maintenance.updateHeartbeat
     */
    fun updateHeartbeat(instance: String = myInstanceName) = runBlocking {
        Maintenance.updateHeartbeat(jobPersistence, instance)
    }

    /**
     * Performs the cancellation check which updates the internal list of jobs to be cancelled.
     * @see Maintenance.checkForCancellations
     */
    fun checkForCancellations(jobCancellationQueue: AtomicReference<Set<String>>) = runBlocking {
        Maintenance.checkForCancellations(jobPersistence, jobCancellationQueue)
    }

    /**
     * Restarts jobs from dead instances.
     * @see Maintenance.restartJobsFromDeadInstances
     */
    fun restartJobsFromDeadInstances() =
        runBlocking { Maintenance.restartJobsFromDeadInstances(jobPersistence, persistencesPerType, maintenanceConfig.heartbeatInterval, maxRestartsPerType) }

    /**
     * Deletes old jobs.
     * @see Maintenance.deleteOldJobs
     */
    fun deleteOldJobs() = runBlocking { Maintenance.deleteOldJobs(jobPersistence, maintenanceConfig.deleteOldJobsAfter, persistencesPerType) }

    /**
     * Resets running jobs of the given [instance], the default is the instance provided in [JobFrameworkTestingMode].
     * @see Maintenance.resetMyRunningJobs
     */
    fun resetMyRunningJobs(instance: String = myInstanceName) =
        runBlocking { Maintenance.resetMyRunningJobs(jobPersistence, instance, persistencesPerType, maxRestartsPerType) }

    /**
     * Cancels the given job.
     */
    fun cancelJob(job: Job): String =
        runBlocking { com.booleworks.kjobs.control.cancelJob(job, jobPersistence) }
}
