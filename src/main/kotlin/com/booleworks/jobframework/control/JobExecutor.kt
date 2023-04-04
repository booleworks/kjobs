// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.jobframework.control

import com.booleworks.jobframework.boundary.Persistence
import com.booleworks.jobframework.data.DefaultJobPrioritizer
import com.booleworks.jobframework.data.ExecutionCapacity
import com.booleworks.jobframework.data.ExecutionCapacityProvider
import com.booleworks.jobframework.data.Job
import com.booleworks.jobframework.data.JobInput
import com.booleworks.jobframework.data.JobPrioritizer
import com.booleworks.jobframework.data.JobResult
import com.booleworks.jobframework.data.JobStatus
import com.booleworks.jobframework.data.TagMatcher
import com.booleworks.jobframework.data.ifError
import com.booleworks.jobframework.data.orQuitWith
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.time.withTimeoutOrNull
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

private typealias CoroutineJob = kotlinx.coroutines.Job

private val log: Logger = LoggerFactory.getLogger(JobExecutor::class.java)

/**
 * The central instance which is responsible to compute jobs.
 *
 * Required parameters are a [persistence] for job access, the [name of this instance][myInstanceName],
 * and the actual [computation] taking a [Job] and its [JobInput]. This computation must always return
 * a result unless it was cancelled (by a timeout or an explicit cancel operation).
 */
class JobExecutor<INPUT, RESULT, IN : JobInput<in INPUT>, RES : JobResult<out RESULT>>(
    private val persistence: Persistence<INPUT, RESULT, IN, RES>,
    private val myInstanceName: String,
    private val computation: suspend (Job, IN) -> RES?,
    private val executionCapacityProvider: ExecutionCapacityProvider,
    private val timeoutComputation: (Job, IN) -> Duration,
    private val jobPrioritizer: JobPrioritizer = DefaultJobPrioritizer,
    private val tagMatcher: TagMatcher = TagMatcher.Any,
    private val failureGenerator: (String, String) -> RES
) {
    /**
     * The main execution routine of the job executor.
     */
    suspend fun execute() = coroutineScope {
        val myCapacity = getExecutionCapacity() ?: return@coroutineScope
        if (!myCapacity.mayTakeJobs) {
            log.debug("No capacity for further jobs.")
            return@coroutineScope
        }
        val (job, jobInput) = getAndReserveJob(myCapacity) ?: return@coroutineScope
        val uuid = job.uuid
        val coroutineJob = launchComputationJob(uuid, job, jobInput)
        launchCancellationCheck(coroutineJob, uuid)
    }

    private suspend fun getExecutionCapacity(): ExecutionCapacity? {
        val allMyRunningJobs = persistence.allJobsOfInstance(JobStatus.RUNNING, myInstanceName).orQuitWith {
            log.warn("Failed to retrieve all running jobs: $it")
            return null
        }
        return executionCapacityProvider(allMyRunningJobs)
    }

    private suspend fun getAndReserveJob(executionCapacity: ExecutionCapacity): Pair<Job, IN>? {
        val job = selectJobWithHighestPriority(executionCapacity)
        if (job != null) {
            log.debug("Job executor selected job: ${job.uuid}")
            job.executingInstance = myInstanceName
            job.startedAt = LocalDateTime.now()
            job.status = JobStatus.RUNNING
            // timeout will be recomputed shortly, but we need to set a timeout for the case that the pod is restarted in between
            // (jobs will not be restarted without a timeout being set)
            job.timeout = LocalDateTime.now().plusMinutes(2)
            persistence.transaction { updateJob(job) }.orQuitWith {
                log.error("Failed to update job with ID ${job.uuid}: $it")
                return null
            }
            val input = persistence.fetchInput(job.uuid).orQuitWith {
                log.error("Could not fetch job input for ID ${job.uuid}: $it")
                return null
            }
            return Pair(job, input)
        } else {
            log.trace("No jobs left to execute.")
            return null
        }
    }

    private suspend fun selectJobWithHighestPriority(executionCapacity: ExecutionCapacity): Job? {
        val result = persistence.allJobsWithStatus(JobStatus.CREATED).orQuitWith {
            log.warn("Job access failed with error: $it")
            return null
        }
        return result.filter { tagMatcher.matches(it) && executionCapacity.isSufficientFor(it) }.let(jobPrioritizer)
    }

    private fun CoroutineScope.launchComputationJob(uuid: String, job: Job, jobInput: IN) = launch {
        // Parsing input may take some time, afterwards, we check if anyone might have "stolen" the job (because of overlapping transactions)
        val executingInstance = persistence.fetchJob(uuid).orQuitWith {
            log.error("Failed to fetch job: $it")
            return@launch
        }.executingInstance
        if (executingInstance != myInstanceName) {
            log.info("Job with ID $uuid was stolen from $myInstanceName by $executingInstance")
            return@launch
        } else {
            val timeout = timeoutComputation(job, jobInput)
            job.timeout = LocalDateTime.now().plusSeconds(timeout.inWholeSeconds)
            persistence.transaction { updateJob(job) }

            val result: RES? = runCatching {
                withTimeoutOrNull(timeout.toJavaDuration()) {
                    computation(job, jobInput)
                }
            }.getOrElse { failureGenerator(job.uuid, "Unexpected exception without further information") }
            if (result == null) {
                log.info("Job with ID $uuid failed to finish in iteration #${job.numRestarts + 1} with timeout $timeout seconds.")
                return@launch
            }
            writeResultToDb(uuid, result)
        }
    }

    private fun CoroutineScope.launchCancellationCheck(coroutineJob: CoroutineJob, uuid: String) = launch {
        while (coroutineJob.isActive) {
            if (Maintenance.jobsToBeCancelled.contains(uuid)) {
                coroutineJob.cancelAndJoin()
                persistence.fetchJob(uuid).onRight { job ->
                    if (job.status == JobStatus.SUCCESS || job.status == JobStatus.FAILURE) {
                        log.info("Job with ID $uuid was cancelled, but finished before the cancellation was processed.")
                    } else {
                        job.status = JobStatus.CANCELLED
                        job.finishedAt = LocalDateTime.now()
                        persistence.transaction { updateJob(job) }.orQuitWith {
                            log.error("Failed to update job with ID $uuid to status CANCELLED: $it")
                            return@launch
                        }
                        log.info("Job with ID $uuid was cancelled successfully.")
                    }
                }
                return@launch
            }
            delay(1.seconds) // TODO make configurable?
        }
    }

    private suspend fun writeResultToDb(id: String, result: RES) {
        val job = persistence.fetchJob(id).orQuitWith {
            log.warn("Job with ID $id was deleted from the database during the computation!")
            return
        }
        if (job.executingInstance != myInstanceName) {
            log.warn("Job with ID $id was stolen from $myInstanceName by ${job.executingInstance} after the computation!")
            if (result.isSuccess() && job.status != JobStatus.SUCCESS) {
                job.executingInstance = myInstanceName
            } else {
                return
            }
        }
        job.finishedAt = LocalDateTime.now()
        job.status = if (result.isSuccess()) JobStatus.SUCCESS else JobStatus.FAILURE
        persistence.transaction {
            persistOrUpdateResult(job, result).orQuitWith {
                // Here it's difficult to tell what we should do with the job, since we don't know why persisting the job failed.
                // Should we do nothing, reset it to CREATED, or set it to FAILURE?
                // Currently, we decide to do nothing and just wait for the cleanup tasks to reset the job.
                // Also, we explicitly log an error, since this situation is generally bad.
                log.error("Failed to persist result for ID $id: $it")
                return@transaction
            }
            updateJob(job).ifError {
                log.error("Failed to update the job for ID $id after finishing the computation. The job will remain in an inconsistent state!")
            }
        }
    }
}
