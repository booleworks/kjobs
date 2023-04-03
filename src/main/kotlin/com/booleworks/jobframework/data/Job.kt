// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.jobframework.data

import java.time.LocalDateTime

/**
 * Contains all relevant information about a job except for its input and result.
 * @param uuid the UUID of this job which was automatically generated
 * @param tags a (possibly empty) list of tags associated with this job
 * @param customInfo optional custom information about this job, it's in the user's
 * responsibility to correctly set and read this information
 * @param priority the priority of this job, _lower values indicate a higher priority_,
 * it is recommended, but not required, to use only positive integers, i.e. the highest
 * priority would be `1`
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
 * Interface for a job input. The idea is that the user is free to store the input as binary
 * (serialized) data or as structured data of type [T].
 */
interface JobInput<T> {
    fun data(): T
    fun serializedData(): ByteArray
}

/**
 * Interface for a job result. The idea is that the user is free to store the result as binary
 * (serialized) data or as structured data of type [T].
 */
interface JobResult<T> {
    val uuid: String
    fun isSuccess(): Boolean
    fun result(): T?
    fun serializedResult(): ByteArray?
    fun error(): String?
}

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
