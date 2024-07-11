// SPDX-License-Identifier: MIT
// Copyright 2023-2024 BooleWorks GmbH

package com.booleworks.kjobs.data

import com.booleworks.kjobs.api.persistence.JobPersistence
import com.booleworks.kjobs.data.JobStatistics.Companion.byType
import com.booleworks.kjobs.data.JobStatistics.Companion.forAllJobs
import java.time.Duration

/**
 * An object containing general information about a list of jobs.
 *
 * There are two factory methods which are already provided:
 * - [forAllJobs] creates the statistics for all jobs in the persistence
 * - [byType] generates the statistics per [job type][Job.type]
 *
 * The statistics contain the overall number of jobs as well as the number of jobs in each [status][JobStatus].
 * Furthermore, the [average waiting time in seconds][avgWaitingTimeInSeconds] (computed from all jobs in
 * states [JobStatus.RUNNING], [JobStatus.SUCCESS], and [JobStatus.FAILURE]) and the
 * [average computation time in seconds][avgComputationTimeInSeconds] of all *successful* jobs are provided.
 */
data class JobStatistics(
    val numJobs: Int,
    val numCreated: Int,
    val numRunning: Int,
    val numSuccess: Int,
    val numFailure: Int,
    val numCancelRequested: Int,
    val numCancelled: Int,
    val avgWaitingTimeInSeconds: Double,
    val avgComputationTimeInSeconds: Double,
) {
    companion object {
        suspend fun forAllJobs(persistence: JobPersistence): JobStatistics = forJobs(persistence.fetchAllJobs().orQuitWith { error(it.message) })

        suspend fun byType(persistence: JobPersistence): Map<String, JobStatistics> =
            persistence.fetchAllJobs().orQuitWith { error(it.message) }.groupBy { it.type }.mapValues { forJobs(it.value) }

        @Suppress("MagicNumber")
        private fun forJobs(jobs: List<Job>): JobStatistics {
            val statusToJobs = jobs.groupBy { it.status }.withDefault { emptyList() }
            val created = statusToJobs.getValue(JobStatus.CREATED).count()
            val running = statusToJobs.getValue(JobStatus.RUNNING).count()
            val success = statusToJobs.getValue(JobStatus.SUCCESS).count()
            val failure = statusToJobs.getValue(JobStatus.FAILURE).count()
            val cancelRequested = statusToJobs.getValue(JobStatus.CANCEL_REQUESTED).count()
            val cancelled = statusToJobs.getValue(JobStatus.CANCELLED).count()
            val numStored = created + running + success + failure + cancelRequested + cancelled
            val avgWaitingTime = listOf(JobStatus.RUNNING, JobStatus.SUCCESS, JobStatus.FAILURE).flatMap { statusToJobs.getValue(it) }
                .filter { it.startedAt != null }
                .map { Duration.between(it.createdAt, it.startedAt).toMillis() }.average() / 1000
            val avgComputationTime = statusToJobs.getValue(JobStatus.SUCCESS)
                .map { Duration.between(it.startedAt, it.finishedAt).toMillis() }.average() / 1000
            return JobStatistics(numStored, created, running, success, failure, cancelRequested, cancelled, avgWaitingTime, avgComputationTime)
        }
    }
}
