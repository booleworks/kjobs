// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.data

import java.time.LocalDateTime

/**
 * Contains all relevant information about a job except for its input and result.
 * @param uuid the UUID of this job which was automatically generated
 * @param type the type of this job
 * @param tags a (possibly empty) list of tags associated with this job
 * @param customInfo optional custom information about this job, it's in the user's
 * responsibility to correctly set and read this information
 * @param priority the priority of this job, _lower values indicate a higher priority_,
 * it is recommended, but not required, to use only positive integers, i.e. the highest
 * priority would be `0`
 * @param createdBy the instance by which this job was created
 * @param createdAt when this job was created
 * @param status the current [JobStatus] of this job
 * @param startedAt when this job was started (if it was already started)
 * @param executingInstance the instance which is executing or executed this job (if
 * it was already started)
 * @param finishedAt when this job was finished
 * @param timeout the current or last timeout of this job (if it was already started)
 * @param numRestarts the number of times this job was restarted
 */
class Job(
    val uuid: String,
    val type: String,
    val tags: List<String>,
    val customInfo: String?,
    val priority: Int,
    val createdBy: String,
    val createdAt: LocalDateTime,
    var status: JobStatus,
    var startedAt: LocalDateTime? = null,
    var executingInstance: String? = null,
    var finishedAt: LocalDateTime? = null,
    var timeout: LocalDateTime? = null,
    var numRestarts: Int = 0
)

/**
 * The result of a job with ID [uuid]. It is guaranteed that either [result] (x)or [error] is non-`null`.
 */
class JobResult<out T> private constructor(val uuid: String, val result: T?, val error: String?) {

    companion object {
        fun <T> success(uuid: String, result: T) = JobResult(uuid, result, null)
        fun <T> error(uuid: String, error: String) = JobResult<T>(uuid, null, error)
    }

    override fun hashCode(): Int {
        var result1 = uuid.hashCode()
        result1 = 31 * result1 + (result?.hashCode() ?: 0)
        result1 = 31 * result1 + (error?.hashCode() ?: 0)
        return result1
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as JobResult<*>
        if (uuid != other.uuid) return false
        if (result != other.result) return false
        return error == other.error
    }

    override fun toString() = "JobResult(uuid='$uuid', result=$result, error=$error)"
}

internal val <T> JobResult<T>.isSuccess: Boolean get() = result != null

/**
 * Enumeration of all job stati:
 * - `CREATED`: The job was created and has not been started (it might also have been started before and run in a timeout).
 * - `RUNNING`: The job is currently being computed.
 * - `SUCCESS`: The job was computed successfully and the result has been stored.
 * - `FAILURE`: The job has failed and the respective error was stored.
 * - `CANCEL_REQUESTED`: Cancellation of this job was requested, the job is still running but will be aborted as soon as possible.
 * - `CANCELLED`: The computation of this job was cancelled by the user.
 */
enum class JobStatus {
    CREATED, RUNNING, SUCCESS, FAILURE, CANCEL_REQUESTED, CANCELLED
}
