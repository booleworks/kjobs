// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.data

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
