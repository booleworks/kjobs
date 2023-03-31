package com.mercedesbenz.jobframework.control

import com.mercedesbenz.jobframework.boundary.Persistence
import com.mercedesbenz.jobframework.data.JobResult
import com.mercedesbenz.jobframework.data.JobStatus
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDateTime

object Maintenance {
    private const val MESSAGE_ABORTED = "Job was aborted due to repeated timeouts"

    private val logger: Logger = LoggerFactory.getLogger(this.javaClass)

    fun <RESULT, RES : JobResult<RESULT>> restartLongRunningJobs(
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
                    persistResult(job, failureGenerator(job.uuid, MESSAGE_ABORTED))
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

    fun deleteOldJobs(persistence: Persistence<*, *, *, *>, afterDays: Int) {
        persistence.allJobsFinishedBefore(LocalDateTime.now().minusDays(afterDays.toLong())).orQuitWith {
            logger.error("Job access failed when trying to delete old jobs: $it")
            return
        }.forEach { job ->
            persistence.transaction { deleteForUuid(job.uuid) }
                .ifError { logger.error("Failed to delete old job with ID ${job.uuid}: $it") }
        }
    }
}
