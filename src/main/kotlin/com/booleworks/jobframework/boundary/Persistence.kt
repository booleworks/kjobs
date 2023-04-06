// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.jobframework.boundary

import com.booleworks.jobframework.data.Heartbeat
import com.booleworks.jobframework.data.Job
import com.booleworks.jobframework.data.JobResult
import com.booleworks.jobframework.data.JobStatus
import com.booleworks.jobframework.data.PersistenceAccessResult
import com.booleworks.jobframework.data.mapResult
import java.time.LocalDateTime

/**
 * General interface for the persistence access (usually some kind of database like Redis or Postgres)
 * where jobs, their inputs, their results, and heartbeats are stored.
 *
 * Any kind of write access must be done by acquiring a [TransactionalPersistence] using [transaction].
 *
 * The job's uuid can be used as primary key for all entities ([Job], [INPUT], and [RESULT]). For the
 * [Heartbeat] the primary key is the [instanceName][Heartbeat.instanceName].
 *
 * _If there will be many jobs in the database and if the database supports it, it may be useful to
 * create indices on the job's [JobStatus]_.
 */
interface Persistence<INPUT, RESULT> {
    /**
     * Opens a new transaction to allow write operations.
     */
    suspend fun transaction(block: suspend TransactionalPersistence<INPUT, RESULT>.() -> Unit): PersistenceAccessResult<Unit>

    /**
     * Fetches the job with the given uuid.
     */
    suspend fun fetchJob(uuid: String): PersistenceAccessResult<Job>

    /**
     * Fetches the job input with the given uuid.
     */
    suspend fun fetchInput(uuid: String): PersistenceAccessResult<INPUT>

    /**
     * Fetches the job result with the given uuid.
     */
    suspend fun fetchResult(uuid: String): PersistenceAccessResult<JobResult<RESULT>>

    /**
     * Fetches all heartbeats since the given date.
     */
    suspend fun fetchHeartBeats(since: LocalDateTime): PersistenceAccessResult<List<Heartbeat>>

    /**
     * Fetches all jobs in the given status.
     */
    suspend fun allJobsWithStatus(status: JobStatus): PersistenceAccessResult<List<Job>>

    /**
     * Fetches all jobs in the given status and from the given instance.
     */
    suspend fun allJobsOfInstance(status: JobStatus, instance: String): PersistenceAccessResult<List<Job>>

    /**
     * Fetches all jobs which have been finished before the given date.
     */
    suspend fun allJobsFinishedBefore(date: LocalDateTime): PersistenceAccessResult<List<Job>>

    /**
     * Fetches all jobs which have a timeout before the given date.
     */
    suspend fun allRunningJobsWithTimeoutBefore(date: LocalDateTime): PersistenceAccessResult<List<Job>> =
        allJobsWithStatus(JobStatus.RUNNING).mapResult { jobs -> jobs.filter { it.timeout!! < date } }
}

/**
 * General interface for the persistence access through a transaction.
 * Read access within a transaction is not supported since Redis does not support it (reads in Redis can
 * only be evaluated after the transaction is successfully executed).
 */
interface TransactionalPersistence<INPUT, RESULT> {
    /**
     * Persists the given job.
     *
     * It is assumed that the job is not yet present, if it is, the behavior (whether the job is overridden,
     * the request is ignored, or an error is returned) is undefined.
     */
    suspend fun persistJob(job: Job): PersistenceAccessResult<Unit>

    /**
     * Persists the given job input.
     *
     * It is assumed that the job is not yet present, if it is, the behavior (whether the input is overridden,
     * the request is ignored, or an error is returned) is undefined.
     */
    suspend fun persistInput(job: Job, input: INPUT): PersistenceAccessResult<Unit>

    /**
     * Persists the given job result. If the result is already present, it should be updated accordingly.
     */
    suspend fun persistOrUpdateResult(job: Job, result: JobResult<RESULT>): PersistenceAccessResult<Unit>

    /**
     * Updates the given job.
     *
     * It is assumed that the job is already present, if it is not, the behavior (whether the job is created,
     * the request is ignored, or an error is returned) is undefined.
     */
    suspend fun updateJob(job: Job): PersistenceAccessResult<Unit>

    /**
     * Deletes the job, job input, and job result (if present) with the given UUID.
     *
     * If anything was not found, the result will still be successful.
     * If any delete operation failed with an error (other than not found), this error will be returned.
     */
    suspend fun deleteForUuid(uuid: String): PersistenceAccessResult<Unit>

    /**
     * Creates or updates the given heartbeat (based on the fact that [Heartbeat.instanceName] is the
     * primary key).
     */
    suspend fun updateHeartbeat(heartbeat: Heartbeat): PersistenceAccessResult<Unit>
}
