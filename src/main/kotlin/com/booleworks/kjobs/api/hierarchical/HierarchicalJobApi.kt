// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.api.hierarchical

import com.booleworks.kjobs.common.getOrElse
import com.booleworks.kjobs.common.unwrapOrReturnFirstError
import com.booleworks.kjobs.control.ComputationResult
import com.booleworks.kjobs.control.JobConfig
import com.booleworks.kjobs.control.cancelJob
import com.booleworks.kjobs.control.submit
import com.booleworks.kjobs.data.Job
import com.booleworks.kjobs.data.JobStatus
import com.booleworks.kjobs.data.PersistenceAccessError
import com.booleworks.kjobs.data.mapResult
import com.booleworks.kjobs.data.orQuitWith
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import kotlin.time.Duration.Companion.seconds

internal val logger = LoggerFactory.getLogger("HierarchicalJobs")

// Implemented by KJobs, used by user
interface HierarchicalJobApi<DEP_INPUT, DEP_RESULT> {
    suspend fun submitDependentJob(input: DEP_INPUT): Job?
    suspend fun waitForDependents(callback: HierarchicalJobCallback<DEP_RESULT>)
    suspend fun listDependentJobs(): List<Job>?
    suspend fun cancelAllRemainingJobs()
}

// Implemented by user, used by KJobs
interface HierarchicalJobCallback<RESULT> {
    /**
     * Called when a dependent job is finished.
     */
    fun dependentJobFinished(uuid: String, status: JobStatus, result: ComputationResult<RESULT>)

    /**
     * Called when all currently submitted dependent jobs are finished.
     *
     * Note that this function may be called repeatedly until the parent job has finished.
     */
    fun allDependentJobsFinished()

    /**
     * Gives notice about an unrecoverable error. All remaining dependent jobs are cancelled.
     */
    fun abortWithError(error: PersistenceAccessError)
}

internal class HierarchicalJobApiImpl<DEP_INPUT, DEP_RESULT>(private val jobConfig: JobConfig<DEP_INPUT, DEP_RESULT>, private val parent: String) :
    HierarchicalJobApi<DEP_INPUT, DEP_RESULT> {
    private val dependents = mutableListOf<String>()
    private val finishedDependants = mutableListOf<String>()

    private val persistence = jobConfig.persistence

    override suspend fun submitDependentJob(input: DEP_INPUT): Job? =
        submit(input, jobConfig).rightOr {
            logger.error("Failed to submit dependent job: ${it.message}")
            return null
        }.also { dependents.add(it.uuid) }

    override suspend fun waitForDependents(callback: HierarchicalJobCallback<DEP_RESULT>) {
        coroutineScope {
            launch {
                do {
                    delay(1.seconds) // TODO make configurable
                    persistence.fetchStati(dependents.toList()).orQuitWith { callback.abortWithError(it); cancelAllRemainingJobs(); return@launch }
                        .let { stati ->
                            dependents.zip(stati).filter { it.second !in setOf(JobStatus.CREATED, JobStatus.RUNNING) }.forEach { (uuid, status) ->
                                dependents.remove(uuid)
                                finishedDependants.add(uuid)
                                val result = when (status) {
                                    JobStatus.SUCCESS, JobStatus.FAILURE -> persistence.fetchResult(uuid)
                                        .mapResult { ComputationResult.Success(it) }
                                        .getOrElse { ComputationResult.Error(it.message) }

                                    else -> ComputationResult.Error("Unexpected Job Status: $status")
                                }
                                callback.dependentJobFinished(uuid, status, result)
                            }
                        }
                    if (dependents.isEmpty()) callback.allDependentJobsFinished()
                    val parentStatus = persistence.fetchJob(parent).orQuitWith { callback.abortWithError(it); cancelAllRemainingJobs(); return@launch }.status
                } while (parentStatus == JobStatus.RUNNING)
            }
        }
    }

    override suspend fun listDependentJobs(): List<Job>? =
        dependents.map { persistence.fetchJob(it) }.unwrapOrReturnFirstError {
            logger.error("Failed to fetch dependent jobs: ${it.value.message}")
            return null
        }.value

    override suspend fun cancelAllRemainingJobs() {
        listDependentJobs()?.forEach { cancelJob(it, persistence) }
    }
}
