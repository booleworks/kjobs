// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.jobframework.control

import com.booleworks.jobframework.boundary.Persistence
import com.booleworks.jobframework.data.JobResult
import com.booleworks.jobframework.data.JobStatus
import com.booleworks.jobframework.data.ifError
import com.booleworks.jobframework.data.orQuitWith
import com.booleworks.jobframework.util.getOrElse
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import kotlin.time.Duration
import kotlin.time.toJavaDuration

/**
 * A collection of maintenance jobs.
 */
object Maintenance {
    private const val MESSAGE_ABORTED = "Job was aborted due to repeated timeouts"

    private val logger: Logger = LoggerFactory.getLogger(Maintenance::class.java)

    var jobsToBeCancelled = setOf<String>()
        private set

    /**
     * Retrieves all jobs from the [persistence] in status [JobStatus.CANCEL_REQUESTED] and writes their
     * uuids to [jobsToBeCancelled].
     */
    suspend fun checkForCancellations(persistence: Persistence<*, *, *, *>) {
        // we don't filter for instance here to also cancel jobs which might have been stolen from us
        jobsToBeCancelled = persistence.allJobsWithStatus(JobStatus.CANCEL_REQUESTED).map { jobs -> jobs.map { it.uuid } }.getOrElse { emptyList() }.toSet()
    }

    /**
     * Checks for jobs in status [JobStatus.RUNNING] which have exceeded their timeout. If the number of restarts
     * is less than [maxRestarts], the job is reset to [JobStatus.CREATED], otherwise the job is set to failure.
     */
    suspend fun <RESULT, RES : JobResult<out RESULT>> restartLongRunningJobs(
        persistence: Persistence<*, RESULT, *, RES>,
        maxRestarts: Int,
        failureGenerator: (String, String) -> RES
    ) {
        val jobsInTimeout = persistence.allRunningJobsWithTimeoutBefore(LocalDateTime.now()).orQuitWith {
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

    /**
     * Deletes all jobs, including their inputs and results, which have finished for longer than the given duration.
     */
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
