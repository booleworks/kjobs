// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.control

import com.booleworks.kjobs.api.DataPersistence
import com.booleworks.kjobs.api.JobPersistence
import com.booleworks.kjobs.common.getOrElse
import com.booleworks.kjobs.data.Heartbeat
import com.booleworks.kjobs.data.JobResult
import com.booleworks.kjobs.data.JobStatus
import com.booleworks.kjobs.data.ifError
import com.booleworks.kjobs.data.orQuitWith
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import kotlin.time.Duration
import kotlin.time.toJavaDuration

/**
 * A collection of maintenance jobs.
 */
object Maintenance {
    private val logger: Logger = LoggerFactory.getLogger(Maintenance::class.java)

    var jobsToBeCancelled = setOf<String>()
        private set

    /**
     * Updates the heartbeat for this instance in the [persistence].
     */
    suspend fun updateHeartbeat(persistence: JobPersistence, myInstanceName: String) {
        persistence.transaction { updateHeartbeat(Heartbeat(myInstanceName, LocalDateTime.now())) }
    }

    /**
     * Retrieves all jobs from the [persistence] in status [JobStatus.CANCEL_REQUESTED] and writes their
     * uuids to [jobsToBeCancelled].
     */
    suspend fun checkForCancellations(persistence: JobPersistence) {
        // we don't filter for instance here to also cancel jobs which might have been stolen from us
        jobsToBeCancelled = persistence.allJobsWithStatus(JobStatus.CANCEL_REQUESTED).map { jobs -> jobs.map { it.uuid } }.getOrElse { emptyList() }.toSet()
    }

    /**
     * Checks for jobs in status [JobStatus.RUNNING] which have exceeded their timeout. If the number of restarts
     * is less than [maxRestarts], the job is reset to [JobStatus.CREATED], otherwise the job is set to failure.
     */
    suspend fun restartJobsFromDeadInstances(
        jobPersistence: JobPersistence,
        specificPersistences: Map<String, DataPersistence<*, *>>,
        pulse: Duration,
        maxRestarts: Int,
    ) {
        val liveInstances = jobPersistence.fetchHeartBeats(LocalDateTime.now().minus((pulse * 2).toJavaDuration())).orQuitWith {
            logger.error("Failed to fetch heartbeats: $it")
            return
        }.map { it.instanceName }.toSet()

        val runningJobs = jobPersistence.allJobsWithStatus(JobStatus.RUNNING).orQuitWith {
            logger.error("Failed to fetch jobs: $it")
            return
        }

        val jobsWithDeadInstances = runningJobs.filter { it.executingInstance !in liveInstances }
        if (jobsWithDeadInstances.isNotEmpty()) {
            val deadInstances = jobsWithDeadInstances.map { it.executingInstance }
            logger.warn("Detected jobs executed by seemingly dead instances. Dead instances are: ${deadInstances.joinToString()}")
        }

        jobsWithDeadInstances.forEach { job ->
            val dataPersistence = specificPersistences[job.type]!!
            dataPersistence.dataTransaction {
                if (job.numRestarts >= maxRestarts) {
                    logger.debug(
                        "Setting job with ID ${job.uuid} to failure because its executing instance seems to be dead " +
                                "and the maximum number of restarts has been reached"
                    )
                    job.status = JobStatus.FAILURE
                    job.finishedAt = LocalDateTime.now()
                    persistOrUpdateResult(job, JobResult.error(job.uuid, "The job was aborted because it exceeded the number of $maxRestarts restarts"))
                } else {
                    logger.debug("Restarting job with ID ${job.uuid} because its executing instance seems to be dead")
                    job.status = JobStatus.CREATED
                    job.numRestarts += 1
                    job.executingInstance = null
                    job.startedAt = null
                    job.timeout = null
                }
                updateJob(job)
            }.ifError { logger.error("Updating job in timeout failed with: $it") }
        }
    }

    /**
     * Deletes all jobs, including their inputs and results, which have finished for longer than the given duration.
     */
    suspend fun deleteOldJobs(persistence: JobPersistence, after: Duration) {
        persistence.allJobsFinishedBefore(LocalDateTime.now().minus(after.toJavaDuration())).orQuitWith {
            logger.error("Failed to fetch jobs: $it")
            return
        }.let { jobs ->
            persistence.transaction { jobs.forEach { deleteForUuid(it.uuid) } }
                .ifError { logger.error("Failed to delete old jobs: $it") }
        }
    }
}
