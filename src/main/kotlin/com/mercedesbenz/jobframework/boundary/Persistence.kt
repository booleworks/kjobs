package com.mercedesbenz.jobframework.boundary

import com.mercedesbenz.jobframework.data.Job
import com.mercedesbenz.jobframework.data.JobInput
import com.mercedesbenz.jobframework.data.JobResult
import com.mercedesbenz.jobframework.data.JobStatus
import com.mercedesbenz.jobframework.data.PersistenceAccessResult
import com.mercedesbenz.jobframework.data.mapResult
import java.time.LocalDateTime

/**
 * General interface for the persistence access where jobs, their inputs, and their results are stored.
 * Any kind of write access must be done by acquiring a [TransactionalPersistence] using [transaction].
 */
interface Persistence<in INPUT, out RESULT, IN : JobInput<in INPUT>, RES : JobResult<out RESULT>> {
    suspend fun transaction(block: suspend TransactionalPersistence<INPUT, RESULT, IN, RES>.() -> Unit): PersistenceAccessResult<Unit>

    suspend fun fetchJob(uuid: String): PersistenceAccessResult<Job>
    suspend fun fetchInput(uuid: String): PersistenceAccessResult<IN>
    suspend fun fetchResult(uuid: String): PersistenceAccessResult<RES>

    suspend fun allJobsFor(status: JobStatus): PersistenceAccessResult<List<Job>>
    suspend fun allJobsOfInstance(status: JobStatus, instance: String): PersistenceAccessResult<List<Job>>
    suspend fun allJobsFinishedBefore(date: LocalDateTime): PersistenceAccessResult<List<Job>>

    suspend fun allRunningJobsWithTimeoutLessThan(date: LocalDateTime): PersistenceAccessResult<List<Job>> =
        allJobsFor(JobStatus.RUNNING).mapResult { jobs -> jobs.filter { it.timeout!! < date } }
}

/**
 * General interface for the persistence access through a transaction.
 * Read access within a transaction is not supported since Redis does not support it (reads in Redis can
 * only be evaluated after the transaction is successfully executed).
 */
interface TransactionalPersistence<in INPUT, out RESULT, IN : JobInput<in INPUT>, RES : JobResult<out RESULT>> {
    suspend fun persistJob(job: Job): PersistenceAccessResult<Unit>
    suspend fun persistInput(job: Job, input: IN): PersistenceAccessResult<Unit>
    suspend fun persistOrUpdateResult(job: Job, result: RES): PersistenceAccessResult<Unit>

    suspend fun updateJob(job: Job): PersistenceAccessResult<Unit>

    /**
     * Deletes the job, job input, and job result (if present) with the given UUID.
     * If anything was not found, the result will still be successful.
     * If any delete operation failed with an error (other than not found), this error will be returned.
     */
    suspend fun deleteForUuid(uuid: String): PersistenceAccessResult<Unit>
}
