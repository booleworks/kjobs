// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.api.hierarchical

import com.booleworks.kjobs.api.JobFrameworkBuilder
import com.booleworks.kjobs.common.getOrElse
import com.booleworks.kjobs.common.unwrapOrReturnFirstError
import com.booleworks.kjobs.control.ComputationResult
import com.booleworks.kjobs.control.cancelJob
import com.booleworks.kjobs.control.submit
import com.booleworks.kjobs.data.Job
import com.booleworks.kjobs.data.JobConfig
import com.booleworks.kjobs.data.JobStatus
import com.booleworks.kjobs.data.PersistenceAccessError
import com.booleworks.kjobs.data.mapResult
import com.booleworks.kjobs.data.orQuitWith
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

internal val logger = LoggerFactory.getLogger("HierarchicalJobs")

class DependentJobException(error: PersistenceAccessError) : Exception(error.message)

/**
 * The API of a hierarchical job. This is an interface provided by KJobs with which the caller can interact within
 * its "super-computation" (provided for example in [JobFrameworkBuilder.addApiForHierarchicalJob]).
 *
 * It allows to [submit dependent jobs][submitDependentJob] and to [wait for their completion][collectDependentResults].
 *
 * The methods [listRemainingDependentJobs] and [cancelAllRemainingJobs] will only be required for more complex use cases.
 */
interface HierarchicalJobApi<DEP_INPUT, DEP_RESULT> {
    /**
     * Submits the given input to create a new dependent job.
     * Such a job will be written to the database as any other job and its computation will be handled as such,
     * i.e. it will be computed once an instance has the capacity to compute it.
     *
     * In order to collect the result(s), use [collectDependentResults].
     * @param input the input for the dependent computation
     */
    suspend fun submitDependentJob(input: DEP_INPUT): Job?

    /**
     * Creates a flow of finished dependent jobs (i.e. jobs *not* in status [JobStatus.CREATED] or [JobStatus.RUNNING]).
     * The flow contains a pair of the job's UUID and its result.
     *
     * This will launch a new coroutine checking every [checkInterval] for newly finished jobs which are then emitted to
     * the returned flow.
     * The flow and the launched coroutine will be cancelled automatically once a terminal operation (like [Flow.collect]
     * or `Flow.first`) is called on it. Also, depending on [cancelRemainingJobsOnFlowCompletion] all remaining jobs will
     * be cancelled upon flow completion. This is especially useful when you are just waiting for the first result to be
     * computed, e.g. via `Flow.first`.
     *
     * **This method should not be called more than once. Doing so will result in undefined behavior.**
     * However, you are free to call this method whenever you want. So [jobs may be submitted][submitDependentJob] before
     * and/or after creating the flow.
     *
     * In case of an unexpected exception, e.g. access to the persistence fails, a [DependentJobException] is thrown.
     *
     * @return a flow containing pairs mapping a job's UUID to its result
     */
    // TODO checkInterval could also be parameterized by the iteration, e.g. to allow to decrease the interval over time
    suspend fun collectDependentResults(
        checkInterval: Duration = 100.milliseconds,
        cancelRemainingJobsOnFlowCompletion: Boolean = true
    ): Flow<Pair<String, ComputationResult<DEP_RESULT>>>

    /**
     * Returns a list of all remaining dependent job. "Remaining" means that the job is still in status [JobStatus.CREATED]
     * or [JobStatus.RUNNING] or it has not yet been collected by [collectDependentResults].
     *
     * In case of an error `null` will be returned. If there are not remaining jobs an empty list is returned.
     */
    suspend fun listRemainingDependentJobs(): List<Job>?

    /**
     * [Cancels][cancelJob] all remaining jobs. See [listRemainingDependentJobs] for a definition of "remaining".
     */
    suspend fun cancelAllRemainingJobs()
}

/**
 * Internal implementation of [HierarchicalJobApi].
 */
internal class HierarchicalJobApiImpl<DEP_INPUT, DEP_RESULT>(private val jobConfig: JobConfig<DEP_INPUT, DEP_RESULT>, private val parent: String) :
    HierarchicalJobApi<DEP_INPUT, DEP_RESULT> {
    private val remainingDependents = mutableListOf<String>()
    private val finishedDependants = mutableListOf<String>()

    private val persistence = jobConfig.persistence

    override suspend fun submitDependentJob(input: DEP_INPUT): Job? =
        submit(input, jobConfig).rightOr {
            logger.error("Failed to submit dependent job: ${it.message}")
            return null
        }.also { remainingDependents.add(it.uuid) }

    override suspend fun collectDependentResults(checkInterval: Duration, cancelRemainingJobsOnFlowCompletion: Boolean) = channelFlow {
        coroutineScope {
            launch(Dispatchers.IO + CoroutineName("Collector of dependent results for hierarchical job $parent")) {
                logger.debug("Launched check of finished dependent jobs for flow emission")
                do {
                    logger.trace("Checking stati of remaining dependent jobs")
                    persistence.fetchStates(remainingDependents.toList())
                        .orQuitWith { logger.error(it.message); cancelAllRemainingJobs(); throw DependentJobException(it) }
                        .let { stati ->
                            logger.trace("Fetched information about ${stati.size} remaining dependent jobs")
                            remainingDependents.zip(stati).filter { it.second !in setOf(JobStatus.CREATED, JobStatus.RUNNING) }.forEach { (uuid, status) ->
                                logger.trace("Found newly finished dependent job with UUID {} in status {}", uuid, status)
                                remainingDependents.remove(uuid)
                                finishedDependants.add(uuid)
                                val result = when (status) {
                                    JobStatus.SUCCESS, JobStatus.FAILURE -> persistence.fetchResult(uuid)
                                        .mapResult { ComputationResult.Success(it) }
                                        .getOrElse { ComputationResult.Error(it.message) }

                                    else -> ComputationResult.Error("Unexpected Job Status: $status")
                                }
                                logger.debug("Emitting result for dependent job with UUID {}: {}", uuid, result)
                                send(uuid to result)
                            }
                        }
                    val parentStatus = persistence.fetchJob(parent).orQuitWith { cancelAllRemainingJobs(); throw DependentJobException(it) }.status
                    delay(checkInterval)
                } while (remainingDependents.isNotEmpty() && parentStatus == JobStatus.RUNNING)
                logger.warn("Parent job with ID $parent finished, but its flow of dependent jobs wasn't collected.")
            }
        }
    }.onCompletion {
        logger.debug("Flow of dependent jobs finished")
        if (cancelRemainingJobsOnFlowCompletion) cancelAllRemainingJobs()
    }

    override suspend fun listRemainingDependentJobs(): List<Job>? =
        remainingDependents.map { persistence.fetchJob(it) }.unwrapOrReturnFirstError {
            logger.error("Failed to fetch dependent jobs: ${it.value.message}")
            return null
        }.value

    override suspend fun cancelAllRemainingJobs() {
        logger.debug("Cancelling all ${remainingDependents.size} remaining jobs")
        listRemainingDependentJobs()?.forEach { cancelJob(it, persistence) }
    }
}
