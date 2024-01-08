// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.data

///**
// * The result of a long poll request. It provides the [PollStatus] and optionally the result
// * or the error of the requested job. Note that, depending on the implementation of the
// * [LongPollManager], `result` or `error` might never be provided (even if the `status` would
// * suggest that one of them is present).
// */
//data class PollResult<RESULT>(val status: PollStatus, val result: RESULT?, val error: String?)

/**
 * The status of a long poll request:
 * - `SUCCESS`: The job finished with [JobStatus.SUCCESS]
 * - `FAILURE`: The job finished with [JobStatus.FAILURE]
 * - `TIMEOUT`: The job is still in status [JobStatus.CREATED] or [JobStatus.RUNNING]
 * - `ABORTED`: The job was canceled or even removed
 */
enum class PollStatus {
    SUCCESS, FAILURE, TIMEOUT, ABORTED;

    companion object {
        fun fromJobStatus(jobStatus: JobStatus) = when (jobStatus) {
            JobStatus.CREATED -> TIMEOUT
            JobStatus.RUNNING -> TIMEOUT
            JobStatus.SUCCESS -> SUCCESS
            JobStatus.FAILURE -> FAILURE
            JobStatus.CANCEL_REQUESTED -> ABORTED
            JobStatus.CANCELLED -> ABORTED
        }
    }
}
