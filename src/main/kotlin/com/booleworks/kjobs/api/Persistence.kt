// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.api

import com.booleworks.kjobs.data.Heartbeat
import com.booleworks.kjobs.data.Job
import com.booleworks.kjobs.data.JobResult
import com.booleworks.kjobs.data.JobStatus
import com.booleworks.kjobs.data.PersistenceAccessResult
import java.time.LocalDateTime

/**
 * General interface for the persistence access (usually some kind of database like Redis or Postgres)
 * where jobs and heartbeats are stored.
 *
 * Any kind of write access must be done by acquiring a [JobTransactionalPersistence] using [transaction].
 *
 * The job's uuid can be used as primary key. For the [Heartbeat] the primary key is the
 * [instanceName][Heartbeat.instanceName].
 *
 * _If there will be many jobs in the database and if the database supports it, it may be useful to
 * create indices on the job's [JobStatus]_.
 */
interface JobPersistence {
    /**
     * Opens a new transaction to allow write operations.
     */
    suspend fun transaction(block: suspend JobTransactionalPersistence.() -> Unit): PersistenceAccessResult<Unit>

    /**
     * Fetches the job with the given uuid.
     */
    suspend fun fetchJob(uuid: String): PersistenceAccessResult<Job>

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
}

/**
 * General interface for the persistence access through a transaction.
 * Read access within a transaction is not supported since Redis does not support it (reads in Redis can
 * only be evaluated after the transaction is successfully executed).
 */
interface JobTransactionalPersistence {
    /**
     * Persists the given job.
     *
     * It is assumed that the job is not yet present, if it is, the behavior (whether the job is overridden,
     * the request is ignored, or an error is returned) is undefined.
     */
    suspend fun persistJob(job: Job): PersistenceAccessResult<Unit>

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
     *
     * Implementation note: The [persistencesPerType] may only be required in certain situations where
     * jobs of different job types are stored in different locations. The
     * [standard redis implementation][com.booleworks.kjobs.api.impl.RedisJobTransactionalPersistence] for
     * instance does not use it.
     */
    suspend fun deleteForUuid(uuid: String, persistencesPerType: Map<String, DataPersistence<*, *>>): PersistenceAccessResult<Unit>

    /**
     * Creates or updates the given heartbeat (based on the fact that [Heartbeat.instanceName] is the
     * primary key).
     */
    suspend fun updateHeartbeat(heartbeat: Heartbeat): PersistenceAccessResult<Unit>
}


/**
 * General interface for the persistence access (usually some kind of database like Redis or Postgres)
 * where inputs and results for specific job types are stored.
 *
 * Any kind of write access must be done by acquiring a [DataTransactionalPersistence] using [dataTransaction].
 *
 * The job's uuid can be used as primary key for all entities.
 */
interface DataPersistence<INPUT, RESULT> : JobPersistence {
    /**
     * Opens a new transaction to allow write operations.
     */
    suspend fun dataTransaction(block: suspend DataTransactionalPersistence<INPUT, RESULT>.() -> Unit): PersistenceAccessResult<Unit>

    /**
     * Fetches the job input with the given uuid.
     */
    suspend fun fetchInput(uuid: String): PersistenceAccessResult<INPUT>

    /**
     * Fetches the job result with the given uuid.
     */
    suspend fun fetchResult(uuid: String): PersistenceAccessResult<JobResult<RESULT>>
}

/**
 * General interface for the persistence access through a transaction.
 * Read access within a transaction is not supported since Redis does not support it (reads in Redis can
 * only be evaluated after the transaction is successfully executed).
 */
interface DataTransactionalPersistence<INPUT, RESULT> : JobTransactionalPersistence {
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
}
