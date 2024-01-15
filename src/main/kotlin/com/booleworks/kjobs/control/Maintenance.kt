// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.control

import com.booleworks.kjobs.api.persistence.DataPersistence
import com.booleworks.kjobs.api.persistence.JobPersistence
import com.booleworks.kjobs.common.getOrElse
import com.booleworks.kjobs.data.Heartbeat
import com.booleworks.kjobs.data.Job
import com.booleworks.kjobs.data.JobStatus
import com.booleworks.kjobs.data.ifError
import com.booleworks.kjobs.data.orQuitWith
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration
import kotlin.time.toJavaDuration

/**
 * This factor multiplied by the `heartbeatInterval` denotes the acceptable time in which a heartbeat
 * may be delayed. The intention is that we allow no more than one missed heartbeat (which would be
 * factor 2) and wait some more time for the job to write the heartbeat in the database.
 */
private const val HEARTBEAT_TIMEOUT_FACTOR = 2.1

/**
 * A collection of maintenance jobs.
 */
object Maintenance {
    private val logger: Logger = LoggerFactory.getLogger("Maintenance")

    /**
     * Updates the heartbeat for this instance in the [persistence].
     */
    suspend fun updateHeartbeat(persistence: JobPersistence, myInstanceName: String) {
        persistence.transaction { updateHeartbeat(Heartbeat(myInstanceName, LocalDateTime.now())) }
    }

    /**
     * Retrieves all jobs from the [persistence] in status [JobStatus.CANCEL_REQUESTED] and writes their
     * uuids to the [jobCancellationQueue]. The queue is simply overridden (which is why it is in an atomic
     * reference).
     */
    suspend fun checkForCancellations(persistence: JobPersistence, jobCancellationQueue: AtomicReference<Set<String>>) {
        // we don't filter for instance here to also cancel jobs which might have been stolen from us
        jobCancellationQueue.set(
            persistence.allJobsWithStatus(JobStatus.CANCEL_REQUESTED).mapRight { jobs -> jobs.map { it.uuid } }.getOrElse { emptyList() }.toSet()
        )
    }

    /**
     * Checks for jobs in status [JobStatus.RUNNING] belonging to instances which seem to be dead. An instance
     * is assumed to be dead *if it missed to update its heartbeat at least twice*.
     * If such a job has had less than [maxRestartsPerType] restarts, the job is reset to [JobStatus.CREATED],
     * otherwise the job is set to failure. (The assumption is multiple restarts of the same job may indicate
     * that the job's computation is responsible for the death of the instance.)
     */
    suspend fun restartJobsFromDeadInstances(
        jobPersistence: JobPersistence,
        persistencesPerType: Map<String, DataPersistence<*, *>>,
        heartbeatInterval: Duration,
        maxRestartsPerType: Map<String, Int>,
    ) {
        val liveInstances =
            jobPersistence.fetchHeartbeats(LocalDateTime.now().minus((heartbeatInterval * HEARTBEAT_TIMEOUT_FACTOR).toJavaDuration())).orQuitWith {
                logger.error("Failed to fetch heartbeats: $it")
                return
            }.map { it.instanceName }.toSet()
        val runningJobs = jobPersistence.allJobsWithStatus(JobStatus.RUNNING).orQuitWith {
            logger.error("Failed to fetch jobs: $it")
            return
        }
        val jobsWithDeadInstances = runningJobs.filter { it.executingInstance !in liveInstances }
        if (jobsWithDeadInstances.isNotEmpty()) {
            logger.warn("Detected jobs executed by seemingly dead instances. Dead instances are: ${jobsWithDeadInstances.joinToString { it.executingInstance ?: "" }}")
        }
        restartJobs(jobsWithDeadInstances, persistencesPerType, maxRestartsPerType, "its executing instance seems to be dead")
    }

    /**
     * Deletes all jobs, including their inputs and results, which have finished for longer than the given duration.
     */
    suspend fun deleteOldJobs(persistence: JobPersistence, after: Duration, persistencesPerType: Map<String, DataPersistence<*, *>>) {
        persistence.allJobsFinishedBefore(LocalDateTime.now().minus(after.toJavaDuration())).orQuitWith {
            logger.error("Failed to fetch jobs: $it")
            return
        }.let { jobs ->
            persistence.transaction { jobs.forEach { deleteForUuid(it.uuid, persistencesPerType) } }
                .ifError { logger.error("Failed to delete old jobs: $it") }
        }
    }

    /**
     * Resets all [running][JobStatus.RUNNING] jobs of this instance to [JobStatus.CREATED].
     *
     * This may be useful on startup after a possible restart where the instance name did not change.
     *
     * If a job was restarted more than [maxRestartsPerType] times, the result is set to failure.
     */
    suspend fun resetMyRunningJobs(
        persistence: JobPersistence,
        myInstanceName: String,
        persistencesPerType: Map<String, DataPersistence<*, *>>,
        maxRestartsPerType: Map<String, Int>,
    ) {
        val myRunningJobs = persistence.allJobsOfInstance(JobStatus.RUNNING, myInstanceName).orQuitWith {
            logger.error("Failed to fetch jobs: $it")
            return
        }
        restartJobs(myRunningJobs, persistencesPerType, maxRestartsPerType, "its instance has been restarted")
    }

    private suspend inline fun restartJobs(
        jobs: List<Job>, specificPersistences: Map<String, DataPersistence<*, *>>, maxRestartsPerType: Map<String, Int>, hint: String
    ) = jobs.forEach { job ->
        restartJob(job, maxRestartsPerType[job.type]!!, hint, specificPersistences[job.type]!!)
    }

    internal suspend inline fun restartJob(job: Job, maxRestarts: Int, hint: String, persistence: DataPersistence<*, *>) {
        persistence.dataTransaction {
            if (job.numRestarts >= maxRestarts) {
                logger.debug("Setting job with ID ${job.uuid} to failure because $hint and the maximum number of restarts has been reached")
                job.status = JobStatus.FAILURE
                job.finishedAt = LocalDateTime.now()
                persistOrUpdateFailure(job, "The job was aborted because it exceeded the maximum number of $maxRestarts restarts")
            } else {
                logger.debug("Restarting job with ID ${job.uuid} because $hint")
                job.status = JobStatus.CREATED
                job.numRestarts += 1
                job.executingInstance = null
                job.startedAt = null
                job.timeout = null
            }
            updateJob(job)
        }.ifError { logger.error("Updating job failed with: $it") }
    }
}
