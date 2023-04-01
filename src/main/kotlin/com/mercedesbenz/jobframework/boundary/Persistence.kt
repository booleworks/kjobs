package com.mercedesbenz.jobframework.boundary

import com.mercedesbenz.jobframework.data.Job
import com.mercedesbenz.jobframework.data.JobInput
import com.mercedesbenz.jobframework.data.JobResult
import com.mercedesbenz.jobframework.data.JobStatus
import java.time.LocalDateTime

class JobAccessResult<R> private constructor(val result: R?, val error: JobAccessError?) {
    val successful = result != null

    inline fun orQuitWith(block: (JobAccessError) -> Nothing): R {
        if (error == null) {
            return result!!
        } else {
            block(error)
        }
    }

    inline fun ifError(block: (JobAccessError) -> Unit) {
        error?.let(block)
    }

    fun <T> mapResult(mapper: (R) -> T): JobAccessResult<T> = JobAccessResult(result?.let(mapper), error)

    companion object {
        val success = JobAccessResult(Unit, null)
        fun <R> result(result: R): JobAccessResult<R> = JobAccessResult(result, null)
        fun <R> notFound(): JobAccessResult<R> = JobAccessResult(null, JobAccessError.NotFound)
        fun <R> internalError(message: String): JobAccessResult<R> = JobAccessResult(null, JobAccessError.InternalError(message))
    }
}

sealed interface JobAccessError {
    object NotFound : JobAccessError {
        override fun toString() = "Not found"
    }

    data class InternalError(val message: String) : JobAccessError {
        override fun toString() = message
    }
}

/**
 * General interface for the persistence access where jobs, their inputs, and their results are stored.
 * Any kind of write access must be done by acquiring a [TransactionalPersistence] using [transaction].
 */
interface Persistence<in INPUT, out RESULT, IN : JobInput<in INPUT>, RES : JobResult<out RESULT>> {
    suspend fun transaction(block: suspend TransactionalPersistence<INPUT, RESULT, IN, RES>.() -> Unit): JobAccessResult<Unit>

    suspend fun fetchJob(uuid: String): JobAccessResult<Job>
    suspend fun fetchInput(uuid: String): JobAccessResult<IN>
    suspend fun fetchResult(uuid: String): JobAccessResult<RES>

    suspend fun allJobsFor(status: JobStatus): JobAccessResult<List<Job>>
    suspend fun allJobsOfInstance(status: JobStatus, instance: String): JobAccessResult<List<Job>>
    suspend fun allJobsFinishedBefore(date: LocalDateTime): JobAccessResult<List<Job>>

    suspend fun allRunningJobsWithTimeoutLessThan(date: LocalDateTime): JobAccessResult<List<Job>> =
        allJobsFor(JobStatus.RUNNING).mapResult { jobs -> jobs.filter { it.timeout!! < date } }
}

/**
 * General interface for the persistence access through a transaction.
 * Read access within a transaction is not supported since Redis does not support it (reads in Redis can
 * only be evaluated after the transaction is successfully executed).
 */
interface TransactionalPersistence<in INPUT, out RESULT, IN : JobInput<in INPUT>, RES : JobResult<out RESULT>> {
    suspend fun persistJob(job: Job): JobAccessResult<Unit>
    suspend fun persistInput(job: Job, input: IN): JobAccessResult<Unit>
    suspend fun persistOrUpdateResult(job: Job, result: RES): JobAccessResult<Unit>

    suspend fun updateJob(job: Job): JobAccessResult<Unit>

    /**
     * Deletes the job, job input, and job result (if present) with the given UUID.
     * If anything was not found, the result will still be successful.
     * If any delete operation failed with an error (other than not found), this error will be returned.
     */
    suspend fun deleteForUuid(uuid: String): JobAccessResult<Unit>
}
