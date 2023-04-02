package com.mercedesbenz.jobframework.control

import com.mercedesbenz.jobframework.boundary.Persistence
import com.mercedesbenz.jobframework.data.JobResult
import com.mercedesbenz.jobframework.data.JobStatus
import com.mercedesbenz.jobframework.data.ifError
import com.mercedesbenz.jobframework.data.orQuitWith
import com.mercedesbenz.jobframework.util.getOrElse
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import kotlin.time.Duration
import kotlin.time.toJavaDuration

object Maintenance {
    private const val MESSAGE_ABORTED = "Job was aborted due to repeated timeouts"

    private val logger: Logger = LoggerFactory.getLogger(Maintenance::class.java)

    var jobsToBeCancelled = setOf<String>()
        private set

    suspend fun checkForCancellations(persistence: Persistence<*, *, *, *>) {
        // we don't filter for instance here to also cancel jobs which might have been stolen from us
        jobsToBeCancelled = persistence.allJobsWithStatus(JobStatus.CANCEL_REQUESTED).map { jobs -> jobs.map { it.uuid } }.getOrElse { emptyList() }.toSet()
    }

    suspend fun <RESULT, RES : JobResult<out RESULT>> restartLongRunningJobs(
        persistence: Persistence<*, RESULT, *, RES>,
        maxRestarts: Int,
        failureGenerator: (String, String) -> RES
    ) {
        val jobsInTimeout = persistence.allRunningJobsWithTimeoutLessThan(LocalDateTime.now()).orQuitWith {
            logger.error("Job access failed when trying to restart long running jobs: $it")
            return
        }
        jobsInTimeout.forEach { job ->
            persistence.transaction {
                if (job.numRestarts >= maxRestarts) {
                    job.status = JobStatus.FAILURE
                    job.finishedAt = LocalDateTime.now()
                    persistOrUpdateResult(job, failureGenerator(job.uuid, MESSAGE_ABORTED))
                } else {
                    job.status = JobStatus.CREATED
                    job.numRestarts += 1
                    job.executingInstance = null
                    job.startedAt = null
                }
                updateJob(job)
            }.ifError { logger.error("Updating job in timeout failed with: $it") }
        }
    }

    suspend fun deleteOldJobs(persistence: Persistence<*, *, *, *>, after: Duration) {
        persistence.allJobsFinishedBefore(LocalDateTime.now().minus(after.toJavaDuration())).orQuitWith {
            logger.error("Job access failed when trying to delete old jobs: $it")
            return
        }.forEach { job ->
            persistence.transaction { deleteForUuid(job.uuid) }
                .ifError { logger.error("Failed to delete old job with ID ${job.uuid}: $it") }
        }
    }
}
