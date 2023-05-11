// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.control

import com.booleworks.kjobs.api.persistence.DataPersistence
import com.booleworks.kjobs.api.persistence.DataTransactionalPersistence
import com.booleworks.kjobs.api.persistence.JobPersistence
import com.booleworks.kjobs.data.ExecutionCapacity
import com.booleworks.kjobs.data.ExecutionCapacityProvider
import com.booleworks.kjobs.data.Job
import com.booleworks.kjobs.data.JobPrioritizer
import com.booleworks.kjobs.data.JobStatus
import com.booleworks.kjobs.data.PersistenceAccessResult
import com.booleworks.kjobs.data.TagMatcher
import com.booleworks.kjobs.data.ifError
import com.booleworks.kjobs.data.orQuitWith
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.time.withTimeoutOrNull
import kotlinx.coroutines.yield
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.toJavaDuration

private val log: Logger = LoggerFactory.getLogger("JobExecutor")

private typealias CoroutineJob = kotlinx.coroutines.Job

sealed interface ComputationResult<out RESULT> {
    data class Success<out RESULT>(val result: RESULT) : ComputationResult<RESULT>
    data class Error(val message: String, val tryRepeat: Boolean = false) : ComputationResult<Nothing>

    fun resultStatus(): JobStatus = if (this is Success) JobStatus.SUCCESS else JobStatus.FAILURE
}

/**
 * The central instance which is responsible to compute jobs.
 */
class MainJobExecutor(
    private val jobPersistence: JobPersistence,
    private val myInstanceName: String,
    private val executionCapacityProvider: ExecutionCapacityProvider,
    private val jobPrioritizer: JobPrioritizer,
    private val tagMatcher: TagMatcher,
    private val specificExecutors: Map<String, SpecificExecutor<*, *>>
) {
    /**
     * The main execution routine of the job executor.
     */
    suspend fun execute() = coroutineScope {
        val myCapacity = getExecutionCapacity() ?: return@coroutineScope
        if (!myCapacity.mayTakeJobs) {
            log.trace("No capacity for further jobs.")
            return@coroutineScope
        }
        val job = getAndReserveJob(myCapacity) ?: return@coroutineScope
        val coroutineJob = with(specificExecutors[job.type]!!) { launchComputationJob(job) }
        launchCancellationCheck(coroutineJob, job.uuid)
    }

    private suspend inline fun getExecutionCapacity(): ExecutionCapacity? {
        val allMyRunningJobs = jobPersistence.allJobsOfInstance(JobStatus.RUNNING, myInstanceName).orQuitWith {
            log.warn("Failed to retrieve all running jobs: $it")
            return null
        }
        log.trace("Found {} currently running jobs of instance {}", allMyRunningJobs.size, myInstanceName)
        return executionCapacityProvider(allMyRunningJobs)
    }

    private suspend inline fun getAndReserveJob(executionCapacity: ExecutionCapacity): Job? {
        val job = selectJobWithHighestPriority(executionCapacity)
        if (job != null) {
            log.debug("Job executor selected job: ${job.uuid}")
            job.executingInstance = myInstanceName
            job.startedAt = LocalDateTime.now()
            job.status = JobStatus.RUNNING
            // timeout will be recomputed shortly, but we need to set a timeout for the case that the pod is restarted in between
            // (jobs will not be restarted without a timeout being set)
            job.timeout = LocalDateTime.now().plusMinutes(2)
            jobPersistence.transaction { updateJob(job) }.orQuitWith {
                log.error("Failed to update job with ID ${job.uuid}: $it")
                return null
            }
            return job
        } else {
            log.trace("No jobs left to execute.")
            return null
        }
    }

    private suspend inline fun selectJobWithHighestPriority(executionCapacity: ExecutionCapacity): Job? {
        val result = jobPersistence.allJobsWithStatus(JobStatus.CREATED).orQuitWith {
            log.warn("Job access failed with error: $it")
            return null
        }
        return result.filter { tagMatcher.matches(it) && executionCapacity.isSufficientFor(it) }.let(jobPrioritizer::invoke)
    }

    private fun CoroutineScope.launchCancellationCheck(coroutineJob: CoroutineJob, uuid: String) = launch {
        while (coroutineJob.isActive) {
            if (Maintenance.jobsToBeCancelled.contains(uuid)) {
                coroutineJob.cancelAndJoin()
                jobPersistence.fetchJob(uuid).onRight { job ->
                    if (job.status == JobStatus.SUCCESS || job.status == JobStatus.FAILURE) {
                        log.info("Job with ID $uuid was cancelled, but finished before the cancellation was processed.")
                    } else {
                        job.status = JobStatus.CANCELLED
                        job.finishedAt = LocalDateTime.now()
                        jobPersistence.transaction { updateJob(job) }.orQuitWith {
                            log.error("Failed to update job with ID $uuid to status CANCELLED: $it")
                            return@launch
                        }
                        log.info("Job with ID $uuid was cancelled successfully.")
                    }
                }
                return@launch
            }
            delay(INTERNAL_CANCELLATION_CHECK_INTERVAL)
        }
    }

    companion object {
        /**
         * The interval in which [Maintenance.jobsToBeCancelled] is checked during the run of a job.
         */
        val INTERNAL_CANCELLATION_CHECK_INTERVAL = 100.milliseconds
    }
}

/**
 * The computation-specific part of the executor.
 */
class SpecificExecutor<INPUT, RESULT>(
    private val myInstanceName: String,
    private val persistence: DataPersistence<INPUT, RESULT>,
    private val computation: suspend (Job, INPUT) -> ComputationResult<RESULT>,
    private val timeoutComputation: (Job, INPUT) -> Duration,
    private val maxRestarts: Int
) {
    internal fun CoroutineScope.launchComputationJob(job: Job) = launch {
        val uuid = job.uuid
        val jobInput = persistence.fetchInput(uuid).orQuitWith {
            log.error("Could not fetch job input for ID ${uuid}: $it")
            return@launch
        }
        // Parsing input may take some time. Afterward, we check if anyone might have "stolen" the job (because of overlapping transactions)
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

            log.trace("Starting computation of job with ID $uuid")
            val result: ComputationResult<RESULT> = runCatching {
                withTimeoutOrNull(timeout.toJavaDuration()) { computation(job, jobInput) }
                    ?: ComputationResult.Error("The job did not finish within the configured timeout of $timeout", tryRepeat = false)
            }.getOrElse {
                yield() // for the case that the coroutine was cancelled
                log.error("The job with ID $uuid failed with an exception and will be set to 'FAILURE': ${it.message}", it)
                ComputationResult.Error("Unexpected exception during computation: ${it.message}", tryRepeat = false)
            }
            log.trace("Finished computation of job with ID $uuid")
            writeResultToDb(uuid, result)
        }
    }

    private suspend inline fun writeResultToDb(id: String, computationResult: ComputationResult<RESULT>) {
        log.trace("Fetching job with ID $id to update its result")
        val job = persistence.fetchJob(id).orQuitWith {
            log.warn("Job with ID $id was deleted from the database during the computation!")
            return
        }
        if (job.executingInstance != myInstanceName) {
            log.warn("Job with ID $id was stolen from $myInstanceName by ${job.executingInstance} after the computation!")
            if (job.status != JobStatus.SUCCESS && computationResult.resultStatus() == JobStatus.SUCCESS) {
                job.executingInstance = myInstanceName
            } else {
                return
            }
        }
        log.trace("Updating job and result for ID $id")
        when (computationResult) {
            is ComputationResult.Error -> {
                if (computationResult.tryRepeat && job.numRestarts < maxRestarts)
                    Maintenance.restartJob(job, maxRestarts, "it failed with ${computationResult.message} and is marked as retry", persistence)
                else {
                    updateResultOrFailure(job, computationResult, id, "failure") { persistOrUpdateFailure(job, computationResult.message) }
                }
            }

            is ComputationResult.Success ->
                updateResultOrFailure(job, computationResult, id, "failure") { persistOrUpdateResult(job, computationResult.result) }
        }
    }

    private suspend inline fun <C : ComputationResult<RESULT>> updateResultOrFailure(
        job: Job, computation: C, id: String, type: String,
        crossinline updateAction: suspend DataTransactionalPersistence<*, RESULT>.() -> PersistenceAccessResult<Unit>
    ) {
        job.finishedAt = LocalDateTime.now()
        job.status = computation.resultStatus()
        persistence.dataTransaction {
            updateAction().orQuitWith {
                // Here it's difficult to tell what we should do with the job, since we don't know why persisting the job failed.
                // Should we do nothing, reset it to CREATED, or set it to FAILURE?
                // Currently, we decide to do nothing and just wait for the cleanup tasks to reset the job.
                // Also, we explicitly log an error, since this situation is generally bad.
                log.error("Failed to persist $type for ID $id: $it")
                return@dataTransaction
            }
            updateJob(job).ifError {
                log.error("Failed to update the job for ID $id after finishing the computation. The job will remain in an inconsistent state! $it")
            }
        }.ifError {
            log.error("Failed to update the job for ID $id after finishing the computation. The job might remain in an inconsistent state! $it")
        }
    }
}
