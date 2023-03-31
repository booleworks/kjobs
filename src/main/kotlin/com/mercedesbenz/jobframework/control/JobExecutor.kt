package com.mercedesbenz.jobframework.control

import com.mercedesbenz.jobframework.boundary.Persistence
import com.mercedesbenz.jobframework.data.Job
import com.mercedesbenz.jobframework.data.JobInput
import com.mercedesbenz.jobframework.data.JobResult
import com.mercedesbenz.jobframework.data.JobStatus
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.time.withTimeoutOrNull
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import kotlin.time.toJavaDuration

interface ExecutionCapacity {
    val mayTakeJobs: Boolean
    fun isSufficientFor(job: Job): Boolean

    companion object {
        object AcceptingAnyJob : ExecutionCapacity {
            override val mayTakeJobs = true
            override fun isSufficientFor(job: Job) = true
        }

        object AcceptingNoJob : ExecutionCapacity {
            override val mayTakeJobs = false
            override fun isSufficientFor(job: Job) = false
        }
    }
}

typealias JobPrioritizer = (List<Job>) -> Job?

val DefaultJobPrioritizer: JobPrioritizer = { jobs -> jobs.minWithOrNull(compareBy({ it.priority }, { it.createdAt })) }

interface TagMatcher {
    fun matches(job: Job): Boolean

    object Any : TagMatcher {
        override fun matches(job: Job) = true
    }

    class OneOf(private val desiredTags: List<String>) : TagMatcher {
        override fun matches(job: Job) = desiredTags.intersect(job.tags.toSet()).isNotEmpty()
    }

    class AllOf(private val desiredTags: List<String>) : TagMatcher {
        override fun matches(job: Job) = job.tags.toSet().containsAll(desiredTags)
    }

    class Exactly(private vararg val desiredTags: String) : TagMatcher {
        override fun matches(job: Job) = job.tags.toSet() == desiredTags.toSet()
    }
}

class JobExecutor<INPUT, RESULT, IN : JobInput<in INPUT>, RES : JobResult<out RESULT>>(
    private val persistence: Persistence<INPUT, RESULT, IN, RES>,
    private val myInstanceName: String,
    private val computation: suspend (Job, IN) -> RES,
    private val executionCapacityProvider: (List<Job>) -> ExecutionCapacity,
    private val timeoutComputation: (Job, IN) -> kotlin.time.Duration,
    private val jobPrioritizer: JobPrioritizer = DefaultJobPrioritizer,
    private val tagMatcher: TagMatcher = TagMatcher.Any,
) {
    suspend fun execute() {
        val myCapacity = getExecutionCapacity() ?: return
        if (!myCapacity.mayTakeJobs) {
            log.debug("No capacity for further jobs.")
        } else {
            val (job, jobInput) = getAndReserveJob(myCapacity) ?: return
            val id = job.uuid
            // Parsing input may take some time, afterwards we check if anyone might have "stolen" the job (because of overlapping transactions)
            val executingInstance = persistence.fetchJob(id).orQuitWith {
                log.error("Failed to fetch job: $it")
                return
            }.executingInstance
            if (executingInstance != myInstanceName) {
                log.info(
                    "Job with ID $id was stolen from $myInstanceName by $executingInstance! " +
                            "(Does not harm in this case, we didn't compute anything so far.)"
                )
                return
            } else {
                val timeout = timeoutComputation(job, jobInput)
                job.timeout = LocalDateTime.now().plusSeconds(timeout.inWholeSeconds)
                persistence.transaction { updateJob(job) }

                val result = coroutineScope {
                    withTimeoutOrNull(timeout.toJavaDuration()) {
                        computation(job, jobInput)
                    }
                }
                if (result == null) {
                    log.info("Job with ID $id failed to finish in iteration #${job.numRestarts + 1} with timeout $timeout seconds.")
                    return
                }
                writeResultToDb(id, result)
            }
        }
    }

    private fun writeResultToDb(id: String, result: RES) {
        val job = persistence.fetchJob(id).orQuitWith {
            log.warn("Job with ID $id was deleted from the database during the computation!")
            return
        }
        if (job.executingInstance != myInstanceName) {
            // TODO Result doch schreiben
            log.warn("Job with ID $id was stolen from $myInstanceName by ${job.executingInstance} after the computation!")
            return
        }
        job.finishedAt = LocalDateTime.now()
        job.status = if (result.isSuccess) JobStatus.SUCCESS else JobStatus.FAILURE
        persistence.transaction {
            persistResult(job, result).orQuitWith {
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

    private fun getExecutionCapacity(): ExecutionCapacity? {
        val allMyRunningJobs = persistence.allJobsOfInstance(JobStatus.RUNNING, myInstanceName).orQuitWith {
            log.warn("Failed to retrieve all running jobs: $it")
            return null
        }
        return executionCapacityProvider(allMyRunningJobs)
    }

    private fun getAndReserveJob(executionCapacity: ExecutionCapacity): Pair<Job, IN>? {
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

    private fun selectJobWithHighestPriority(executionCapacity: ExecutionCapacity): Job? {
        val result = persistence.allJobsFor(JobStatus.CREATED).orQuitWith {
            log.warn("Job access failed with error: $it")
            return null
        }
        return result.filter { tagMatcher.matches(it) && executionCapacity.isSufficientFor(it) }.let(jobPrioritizer)
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(this::class.java)
    }
}
