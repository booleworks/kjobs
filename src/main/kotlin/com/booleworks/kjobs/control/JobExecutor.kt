// SPDX-License-Identifier: MIT
// Copyright 2023-2024 BooleWorks GmbH

package com.booleworks.kjobs.control

import com.booleworks.kjobs.api.JobFrameworkBuilder.CancellationConfig
import com.booleworks.kjobs.api.persistence.DataPersistence
import com.booleworks.kjobs.api.persistence.DataTransactionalPersistence
import com.booleworks.kjobs.api.persistence.JobPersistence
import com.booleworks.kjobs.common.getOrElse
import com.booleworks.kjobs.control.polling.LongPollManager
import com.booleworks.kjobs.data.ExecutionCapacity
import com.booleworks.kjobs.data.ExecutionCapacityProvider
import com.booleworks.kjobs.data.Job
import com.booleworks.kjobs.data.JobPrioritizer
import com.booleworks.kjobs.data.JobStatus
import com.booleworks.kjobs.data.PersistenceAccessError
import com.booleworks.kjobs.data.PersistenceAccessResult
import com.booleworks.kjobs.data.TagMatcher
import com.booleworks.kjobs.data.ifError
import com.booleworks.kjobs.data.orQuitWith
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.time.withTimeoutOrNull
import kotlinx.coroutines.withContext
import kotlinx.coroutines.yield
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

private val log: Logger = LoggerFactory.getLogger("com.booleworks.kjobs.JobExecutor")

private const val NUMBER_OF_JOB_RESERVATION_RETRIES = 10 // TODO should we make this configurable?

private typealias CoroutineJob = kotlinx.coroutines.Job

sealed interface ComputationResult<out RESULT> {
    data class Success<out RESULT>(val result: RESULT) : ComputationResult<RESULT>
    data class Error(val message: String, val tryRepeat: Boolean = false) : ComputationResult<Nothing>

    fun resultStatus(): JobStatus = if (this is Success) JobStatus.SUCCESS else JobStatus.FAILURE
}

/**
 * Thread-safe pool of the currently running jobs of this instance.
 */
class JobExecutionPool {
    /**
     * The pool of running jobs is stored as map from job ID to the job instance.
     */
    private val runningJobs = ConcurrentHashMap<String, Job>()

    /**
     * Adds the given job to the pool of running jobs.
     * Returns the job instance associated with the job ID of the given job, or `null` if there was no job present for this job ID yet.
     */
    fun addJob(job: Job): Job? {
        log.debug("Adding job to JobExecutionPool: ${job.uuid}")
        return runningJobs.put(job.uuid, job)
    }

    /**
     * Removes the job associated with the given job ID.
     * Returns the job instance associated with the given job ID, or `null` if the job was not present for this job ID.
     */
    fun removeJob(uuid: String): Job? {
        log.debug("Removing job from JobExecutionPool: $uuid")
        return runningJobs.remove(uuid)
    }

    /**
     * Returns all running jobs.
     */
    fun getAll(): List<Job> = runningJobs.values.toList()
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
    private val cancellationConfig: CancellationConfig,
    private val jobCancellationQueue: AtomicReference<Set<String>>,
    private val specificExecutors: Map<String, SpecificExecutor<*, *>>,
    private val jobExecutionPool: JobExecutionPool = JobExecutionPool(),
) {
    /**
     * The main execution routine of the job executor.
     *
     * Note that the actual computation is started in a separate coroutine
     * job (i.e. not a child job of the outer context). This computation job
     * is returned if the computation was started.
     * @param computationDispatcher the dispatcher to be used for the actual computations
     * @return the coroutine job of the actual computation
     */
    suspend fun execute(computationDispatcher: CoroutineDispatcher): kotlinx.coroutines.Job? = coroutineScope {
        launch { Maintenance.updateHeartbeat(jobPersistence, myInstanceName) }
        val myCapacity = getExecutionCapacity()
        if (!myCapacity.mayTakeJobs) {
            log.trace("No capacity for further jobs.")
            return@coroutineScope null
        }
        val job = getAndReserveJob(myCapacity) ?: return@coroutineScope null
        val computationJob = with(specificExecutors[job.type]!!) { launchComputationJob(job, computationDispatcher) }
        computationJob.invokeOnCompletion { throwable ->
            throwable?.let { log.warn("Exception while running job ${job.uuid} with message '${throwable.message}'", throwable) }
            jobExecutionPool.removeJob(job.uuid)
        }
        if (cancellationConfig.enabled) {
            launchCancellationCheck(computationJob, jobCancellationQueue, job.uuid)
        }
        return@coroutineScope computationJob
    }

    private fun getExecutionCapacity(): ExecutionCapacity {
        val allMyRunningJobs = jobExecutionPool.getAll()
        log.trace("Found {} currently running jobs of instance {}", allMyRunningJobs.size, myInstanceName)
        return executionCapacityProvider(allMyRunningJobs)
    }

    private suspend inline fun getAndReserveJob(executionCapacity: ExecutionCapacity): Job? {
        val jobs = getRelevantJobs(executionCapacity)
        return selectJobToExecute(jobs, NUMBER_OF_JOB_RESERVATION_RETRIES)
    }

    private suspend fun getRelevantJobs(executionCapacity: ExecutionCapacity): List<Job> {
        log.trace("Fetching relevant jobs.")
        val jobs = jobPersistence.allJobsWithStatus(JobStatus.CREATED).orQuitWith {
            log.error("Job access failed with error: $it")
            return listOf()
        }.filter { tagMatcher.matches(it) && executionCapacity.isSufficientFor(it) }.toList()
        log.trace("Found relevant Jobs: ${jobs.size}")
        return jobs
    }

    private suspend inline fun selectJobToExecute(jobs: List<Job>, tryLimit: Int): Job? {
        val jobCandidates = jobs.associateBy { it.uuid }.toMutableMap()
        var counter = 0
        while (counter < tryLimit) {
            val currentJob = selectJobWithHighestPriority(jobCandidates.values.toList())
            if (currentJob == null) {
                log.trace("No suitable job found in job candidates to reserve.")
                return null
            }
            log.trace("Selected Job with highest priority: ${currentJob.uuid}")
            val reservationResult = tryReserveJob(currentJob)
            when (reservationResult) {
                JobReservationResult.SUCCESS -> return currentJob
                JobReservationResult.ERROR -> return null
                JobReservationResult.ALREADY_RESERVED -> {
                    counter++
                    jobCandidates.remove(currentJob.uuid)
                }
            }
        }
        log.trace("Tried $counter times to reserve a job but did not succeed.")
        return null
    }

    private fun selectJobWithHighestPriority(jobs: List<Job>): Job? = jobs.let(jobPrioritizer::invoke)

    private suspend inline fun tryReserveJob(job: Job): JobReservationResult {
        job.executingInstance = myInstanceName
        job.startedAt = LocalDateTime.now()
        job.status = JobStatus.RUNNING
        // timeout will be recomputed shortly, but we need to set a timeout for the case that the pod is restarted in between
        // (jobs will not be restarted without a timeout being set)
        job.timeout = LocalDateTime.now().plusMinutes(2)
        val jobInCreatedStatus: Map<String, (Job) -> Boolean> = mapOf(job.uuid to { it.status == JobStatus.CREATED })
        jobPersistence.transactionWithPreconditions(jobInCreatedStatus) { updateJob(job) }
            .ifError {
                if (it == PersistenceAccessError.Modified) {
                    log.debug("Job with ID ${job.uuid} was modified before it could be reserved. Another instance was probably faster.")
                    return JobReservationResult.ALREADY_RESERVED
                } else {
                    log.error("Failed to update job with ID ${job.uuid}: $it")
                    return JobReservationResult.ERROR
                }
            }
        jobExecutionPool.addJob(job)
        log.debug("Job successfully reserved job: ${job.uuid}")
        return JobReservationResult.SUCCESS
    }

    private enum class JobReservationResult() { SUCCESS, ERROR, ALREADY_RESERVED }

    private fun launchCancellationCheck(coroutineJob: CoroutineJob, jobCancellationQueue: AtomicReference<Set<String>>, uuid: String) {
        CoroutineScope(Dispatchers.IO + CoroutineName("Cancellation check for job $uuid")).launch {
            while (coroutineJob.isActive) {
                if (jobCancellationQueue.get().contains(uuid)) {
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
                delay(cancellationConfig.checkInterval)
            }
        }
    }
}

/**
 * The computation-specific part of the executor.
 */
class SpecificExecutor<INPUT, RESULT>(
    private val myInstanceName: String,
    private val persistence: DataPersistence<INPUT, RESULT>,
    private val computation: suspend (Job, INPUT) -> ComputationResult<RESULT>,
    private val longPollManager: LongPollManager,
    private val timeoutComputation: (Job, INPUT) -> Duration,
    private val maxRestarts: Int
) {
    internal fun launchComputationJob(job: Job, computationDispatcher: CoroutineDispatcher) =
        CoroutineScope(computationDispatcher + CoroutineName("Computation of Job ${job.uuid}")).launch {
            val uuid = job.uuid
            val jobInput = withContext(Dispatchers.IO) { // input may be large
                persistence.fetchInput(uuid)
            }.orQuitWith {
                abortComputationWithError(persistence, uuid, "Failed to fetch job input for ID ${uuid}: $it", "Failed to fetch job input: $it")
                return@launch
            }
            // Parsing input may take some time. Afterward, we check if anyone might have "stolen" the job (because of overlapping transactions)
            val executingInstance = persistence.fetchJob(uuid).orQuitWith {
                abortComputationWithError(persistence, uuid, "Failed to fetch job: $it", "Failed to fetch job: $it")
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
            when (it) {
                is PersistenceAccessError.InternalError -> {
                    abortComputationWithError(persistence, id, "Failed to fetch job with ID ${id}: $it", "Failed to fetch job: $it")
                }

                PersistenceAccessError.NotFound, is PersistenceAccessError.UuidNotFound -> {
                    log.warn("Job with ID $id was deleted from the database during the computation!")
                }

                is PersistenceAccessError.Modified -> error("Unexpected return value")
            }
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
                updateResultOrFailure(job, computationResult, id, "result") { persistOrUpdateResult(job, computationResult.result) }
        }
    }

    private suspend inline fun <C : ComputationResult<RESULT>> updateResultOrFailure(
        job: Job, computation: C, id: String, type: String,
        crossinline updateData: suspend DataTransactionalPersistence<*, RESULT>.() -> PersistenceAccessResult<Unit>
    ) {
        job.finishedAt = LocalDateTime.now()
        job.status = computation.resultStatus()
        persistence.dataTransaction {
            updateData().orQuitWith {
                // Here it's difficult to tell what we should do with the job, since we don't know why persisting the job failed.
                // Should we do nothing, reset it to CREATED, or set it to FAILURE?
                // Currently, we decide to do nothing and just wait for the cleanup tasks to reset the job.
                // Also, we explicitly log an error, since this situation is generally bad.
                abortComputationWithError(persistence, id, "Failed to persist $type for ID $id: $it", "Failed to persist $type")
                return@dataTransaction
            }
            updateJob(job).ifError {
                abortComputationWithError(
                    persistence, id,
                    "Failed to update the job for ID $id after finishing the computation: $it",
                    "Failed to update the job after finishing the computation"
                )
            }
        }.onRight {
            when (computation.resultStatus()) {
                JobStatus.SUCCESS -> longPollManager.publishSuccess(job.uuid)
                JobStatus.FAILURE -> longPollManager.publishFailure(job.uuid)
                else -> error("Unexpected result status")
            }
        }.ifError {
            abortComputationWithError(
                persistence, id,
                "Failed to update the job for ID $id after finishing the computation: $it",
                "Failed to update the job after finishing the computation"
            )
        }
    }

    private suspend fun abortComputationWithError(persistence: DataPersistence<INPUT, RESULT>, uuid: String, logMessage: String, resultMessage: String) {
        log.error(logMessage)
        log.info("Trying to set job with ID $uuid to `FAILURE`")
        for (i in 1..UPDATE_RETRIES_ON_PERSISTENCE_ERROR) {
            val updateSuccess = persistence.dataTransaction {
                persistence.fetchJob(uuid).map({ false }) { job ->
                    updateJob(job.copy(status = JobStatus.FAILURE, finishedAt = LocalDateTime.now())).orQuitWith { log.error(it.message); return@map false }
                    persistOrUpdateFailure(job, resultMessage).orQuitWith { log.error(it.message); return@map false }
                    true
                }
            }.getOrElse { false }
            if (!updateSuccess) {
                if (i >= UPDATE_RETRIES_ON_PERSISTENCE_ERROR) {
                    log.error("Update failed $UPDATE_RETRIES_ON_PERSISTENCE_ERROR times. Giving up. The application might remain in an inconsistent state.")
                }
                val waitPeriod = i * UPDATE_RETRIES_WAIT_PERIOD
                log.error("Update failed, trying again in $waitPeriod seconds")
                delay(waitPeriod.seconds)
            }
        }
    }

    companion object {
        const val UPDATE_RETRIES_ON_PERSISTENCE_ERROR = 10
        const val UPDATE_RETRIES_WAIT_PERIOD = 5
    }
}
