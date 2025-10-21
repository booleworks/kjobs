// SPDX-License-Identifier: MIT
// Copyright 2023-2024 BooleWorks GmbH

package com.booleworks.kjobs.control

import com.booleworks.kjobs.api.JobFrameworkBuilder
import com.booleworks.kjobs.api.persistence.DataPersistence
import com.booleworks.kjobs.api.persistence.JobPersistence
import com.booleworks.kjobs.common.Either
import com.booleworks.kjobs.data.ApiConfig
import com.booleworks.kjobs.data.Job
import com.booleworks.kjobs.data.JobConfig
import com.booleworks.kjobs.data.JobInfoConfig
import com.booleworks.kjobs.data.JobStatus
import com.booleworks.kjobs.data.PersistenceAccessError
import com.booleworks.kjobs.data.PersistenceAccessResult
import com.booleworks.kjobs.data.PollStatus
import com.booleworks.kjobs.data.SynchronousResourceConfig
import com.booleworks.kjobs.data.ifError
import com.booleworks.kjobs.data.mapResult
import com.booleworks.kjobs.data.orQuitWith
import io.ktor.http.HttpStatusCode
import io.ktor.http.HttpStatusCode.Companion.BadRequest
import io.ktor.http.HttpStatusCode.Companion.InternalServerError
import io.ktor.http.HttpStatusCode.Companion.NotFound
import io.ktor.http.HttpStatusCode.Companion.OK
import io.ktor.server.application.ApplicationCall
import io.ktor.server.response.respond
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.UUID
import kotlin.time.Duration.Companion.milliseconds

private val apiLog = LoggerFactory.getLogger("com.booleworks.kjobs.ApiLog")

/**
 * Setup all routings according to a given [ApiConfig] and [JobConfig].
 *
 * The following routes will be created:
 *
 * ### `POST submit`
 * - receives its input using [ApiConfig.inputReceiver]
 * - optionally validates the input using [ApiConfig.inputValidation] and returns respective error
 * messages with status 400 if it fails
 * - creates a new [Job] stores the job and the input in the [persistence][JobConfig.persistence]
 * - returns the UUID of the generated job
 *
 * ### `GET status/{uuid}`
 * - returns the status of the job with the given uuid
 * - if no such job exists, status 404 is returned
 *
 * ### `GET result/{uuid}`
 * - returns the result using [ApiConfig.resultResponder]
 * - if no such job exists, or it is not in status `SUCCESS`, status 404 is returned
 * - the job is deleted afterward if [ApiConfig.deleteJobAfterFetchingResult] is enabled
 *
 * ### `GET failure/{uuid}`
 * - returns the failure of the job with the given uuid
 * - if no such job exists, or it is not in status `FAILURE`, status 404 is returned
 * - the job is deleted afterward if [ApiConfig.deleteJobAfterFetchingResult] is enabled
 *
 * ### `POST cancel/{uuid}` (if enabled via [JobFrameworkBuilder.CancellationConfig.enabled])
 * - cancels the job with the given uuid
 * - if no such job exists, status 404 is returned
 * - if the job is in status `CREATED`, it is cancelled immediately
 * - if the job is in status `CANCEL_REQUESTED`, `CANCELLED`, `SUCCESS`, or `FAILURE`, the request is ignored
 *     with http status 200 and a respective message
 * - if the job is in status `RUNNING` it will go to `CANCEL_REQUESTED`. As soon as the computation is aborted
 *     the job will change to status `CANCELLED`. However, if the computation finishes (with `SUCCESS` or
 *     `FAILURE`) before it is aborted, the respecting finishing status will be set instead of `CANCELLED`
 *     and the result (or error) will be stored.
 *
 * ### `POST synchronous` (if enabled via [SynchronousResourceConfig.enabled])
 * - performs the same steps as `submit` but does not return the uuid
 * - waits until the job is computed (computation might be performed by another instance)
 * - returns the result of the computation using [ApiConfig.resultResponder]
 * - the default path `synchronous` can be changed via [ApiConfig.syncRoute]
 * - the job is deleted afterward if [ApiConfig.deleteJobAfterFetchingResult] is enabled
 *
 * ### `DELETE delete/{uuid}` (if enabled via [ApiConfig.enableDeletion])
 * - deletes the job with the given uuid
 * - the job must have the `jobType` of this API, otherwise status 400 is returned. So only jobs belonging
 *     to this API can be deleted.
 * - the job must not be in one of the states `CREATED`, `RUNNING`, or `CANCEL_REQUESTED`. Such jobs must be
 *     canceled first (and/or it has to be waited until the cancellation has succeeded and the status is
 *     `CANCELLED`)
 *
 * ### `GET info/{uuid}` (if enabled via [JobInfoConfig.enabled])
 * - returns information about the job with the given uuid
 * - the default path `info` can be changed via [ApiConfig.infoRoute]
 * - the way this information is returned is defined by [JobInfoConfig.responder]
 *
 * If the persistence access in any of the routes fails, status 500 with a respective message is returned.
 *
 * The paths or definitions of these routes can be overridden using the `ApiConfigBuilder.*Route` methods.
 */
internal fun <INPUT, RESULT> Route.setupJobApi(apiConfig: ApiConfig<INPUT, RESULT>, jobConfig: JobConfig<INPUT, RESULT>) = with(apiConfig) {
    submitRoute {
        validateAndSubmit(apiConfig, jobConfig, inputReceiver())?.let { respond(it) }
    }

    statusRoute {
        parseUuid()?.let { resultStatus(it, jobConfig.jobType, jobConfig.persistence) }
    }

    resultRoute {
        parseUuid()?.let { uuid ->
            val job = fetchJobAndCheckType(uuid, jobConfig.jobType, jobConfig.persistence) ?: return@resultRoute
            if (job.status != JobStatus.SUCCESS) {
                respondText("Cannot return the result for job with ID $uuid and status ${job.status}.", status = BadRequest)
            } else {
                result(job.uuid, apiConfig, jobConfig)
            }
        }
    }

    failureRoute {
        parseUuid()?.let { uuid ->
            val job = fetchJobAndCheckType(uuid, jobConfig.jobType, jobConfig.persistence) ?: return@failureRoute
            if (job.status != JobStatus.FAILURE) {
                respondText("Cannot return a failure for job with ID $uuid and status ${job.status}.", status = BadRequest)
            } else {
                failure(job.uuid, apiConfig, jobConfig, OK)
            }
        }
    }

    if (enableDeletion) {
        deleteRoute {
            parseUuid()?.let { uuid ->
                fetchJobAndCheckType(uuid, jobConfig.jobType, jobConfig.persistence)?.let { job ->
                    if (job.type != jobConfig.jobType) {
                        respond(
                            BadRequest,
                            "The job with ID $uuid belongs to another job type. Please call the delete resource with the correct path."
                        )
                    } else if (job.status in setOf(JobStatus.CREATED, JobStatus.RUNNING, JobStatus.CANCEL_REQUESTED)) {
                        respond(BadRequest, "Cannot delete job with ID $uuid, because it is in a wrong status: ${job.status}")
                    } else {
                        jobConfig.persistence.transaction { deleteForUuid(uuid.toString(), mapOf(jobConfig.jobType to jobConfig.persistence)) }
                            .onLeft { respond(InternalServerError, "Deletion of Job with ID $uuid failed with: $it") }
                            .onRight { respond("Deleted") }
                    }
                }
            }
        }
    }

    if (enableCancellation) {
        cancelRoute {
            parseUuid()?.let { uuid ->
                val job = fetchJobAndCheckType(uuid, jobConfig.jobType, jobConfig.persistence) ?: return@cancelRoute
                cancelJob(job, jobConfig.persistence)
                    .onLeft { respond(InternalServerError, it) }
                    .onRight { respond(it) }
            }
        }
    }

    if (syncMockConfig.enabled) {
        syncRoute {
            val uuid = validateAndSubmit(apiConfig, jobConfig.copy(priorityProvider = syncMockConfig.priorityProvider), inputReceiver()) ?: return@syncRoute
            apiLog.trace("Submitted job for synchronous computation, got ID $uuid")
            var status: JobStatus
            val timeout = LocalDateTime.now().plus(syncMockConfig.maxWaitingTime.inWholeMilliseconds, ChronoUnit.MILLIS)
            do {
                delay(syncMockConfig.checkInterval)
                status = jobConfig.persistence.fetchJob(uuid).orQuitWith {
                    respondText("Synchronous computation failed during job access: ${it.message}", status = InternalServerError)
                    return@syncRoute
                }.status
                apiLog.trace("Synchronous job with ID {} is in status {}", uuid, status)
            } while (status in setOf(JobStatus.CREATED, JobStatus.RUNNING) && LocalDateTime.now() < timeout)
            if (status == JobStatus.SUCCESS) {
                result(uuid, apiConfig, jobConfig)
            } else if (status == JobStatus.FAILURE) {
                failure(uuid, apiConfig, jobConfig, InternalServerError)
            } else {
                if (LocalDateTime.now() > timeout) {
                    respondText(
                        "The job did not finish within the timeout of ${syncMockConfig.maxWaitingTime}. " +
                                "You may be able to retrieve the result later via the asynchronous API using the job id $uuid.", status = BadRequest
                    )
                } else {
                    respondText("Job finished in an unexpected status $status", status = InternalServerError)
                }
            }
        }
    }

    if (jobInfoConfig.enabled) {
        infoRoute {
            parseUuid()?.let { uuid ->
                val job = fetchJobAndCheckType(uuid, jobConfig.jobType, jobConfig.persistence) ?: return@infoRoute
                with(jobInfoConfig) { this@infoRoute.responder(job) }
            }
        }
    }

    if (longPollingConfig.enabled) {
        val longPollManager = longPollingConfig.longPollManager()
        longPollingRoute {
            parseUuid()?.let { uuid ->
                val timeout = minOf(
                    request.queryParameters["timeout"]?.toIntOrNull()?.milliseconds ?: longPollingConfig.connectionTimeout,
                    longPollingConfig.connectionTimeout
                )
                // We start to polling before checking the job status to reduce the risk of missing a job update between checking
                // the job and starting to poll (in the worst case we might still miss an update if setting up the poll is slower than
                // checking the job).
                val asyncPoll = with(longPollManager) { subscribe(uuid.toString(), timeout) }
                apiLog.trace("Async Poll for ID {} initiated with timeout {}", uuid, timeout)
                val job = fetchJobAndCheckType(uuid, jobConfig.jobType, jobConfig.persistence) ?: run {
                    asyncPoll.cancelAndJoin()
                    return@longPollingRoute
                }
                val currentStatus = PollStatus.fromJobStatus(job.status)
                apiLog.trace("Awaiting poll result for ID {}", uuid)
                val response = if (currentStatus != PollStatus.TIMEOUT) {
                    apiLog.debug("Status for ID {} already present, cancelling poll", uuid)
                    asyncPoll.cancelAndJoin()
                    currentStatus.toString()
                } else {
                    asyncPoll.await().toString()
                }
                respondText(response)
            }
        }
    }
}

internal suspend inline fun cancelJob(job: Job, persistence: JobPersistence): Either<String, String> {
    when (job.status) {
        JobStatus.CREATED                                         -> {
            apiLog.info("Cancelling job with ID ${job.uuid} from status CREATED")
            job.status = JobStatus.CANCELLED
            job.finishedAt = LocalDateTime.now()
            val jobInStatusCreated: Map<String, (Job) -> Boolean> = mapOf(job.uuid to { it.status == JobStatus.CREATED })
            val cancelResult = persistence.transactionWithPreconditions(jobInStatusCreated) { updateJob(job) }
            return if (cancelResult.isRight) {
                Either.Right("Job with id ${job.uuid} was cancelled successfully")
            } else {
                if ((cancelResult as? Either.Left)?.value is PersistenceAccessError.Modified) {
                    apiLog.warn("Job with ID ${job.uuid} was modified before it could be cancelled")
                } else {
                    apiLog.warn("Cancelling job with ID ${job.uuid} failed with ${(cancelResult as? Either.Left)?.value}")
                }
                Either.Left("Cancellation failed")
            }
        }

        JobStatus.RUNNING                                         -> {
            apiLog.info("Cancelling job with ID ${job.uuid} from status RUNNING")
            job.status = JobStatus.CANCEL_REQUESTED
            val jobInStatusRunning: Map<String, (Job) -> Boolean> = mapOf(job.uuid to { it.status == JobStatus.RUNNING })
            val cancelResult = persistence.transactionWithPreconditions(jobInStatusRunning) { updateJob(job) }
            return if (cancelResult.isRight) {
                Either.Right(
                    "Job with id ${job.uuid} is currently running and will be cancelled as soon as possible. " +
                            "If it finishes in the meantime, the cancel request will be ignored."
                )
            } else {
                if ((cancelResult as? Either.Left)?.value is PersistenceAccessError.Modified) {
                    apiLog.warn("Job with ID ${job.uuid} was modified before it could be cancelled")
                } else {
                    apiLog.warn("Cancelling job with ID ${job.uuid} failed with ${(cancelResult as? Either.Left)?.value}")
                }
                Either.Left("Cancellation failed")
            }
        }

        JobStatus.CANCEL_REQUESTED                                -> {
            apiLog.warn("Detected a duplicate cancellation request for job with ID ${job.uuid}")
            return Either.Right("Cancellation for job with id ${job.uuid} has already been requested")
        }

        JobStatus.SUCCESS, JobStatus.FAILURE, JobStatus.CANCELLED -> {
            if (job.status == JobStatus.CANCELLED) apiLog.warn("Detected a duplicate cancellation request for job with ID ${job.uuid}")
            else apiLog.warn("Got a cancellation request for already finished job in status ${job.status} with ID ${job.uuid}")
            return Either.Right("Job with id ${job.uuid} has already finished with status ${job.status}")
        }
    }
}

internal suspend inline fun <INPUT> submit(
    input: INPUT,
    jobConfig: JobConfig<INPUT, *>
): PersistenceAccessResult<Job> = with(jobConfig) {
    val uuid = UUID.randomUUID().toString()
    val tags = tagProvider(input)
    val customInfo = customInfoProvider(input)
    val job = Job(uuid, jobType, tags, customInfo, priorityProvider(input), myInstanceName, LocalDateTime.now(), JobStatus.CREATED)
    apiLog.trace("Persisting job with ID $uuid")
    return persistence.dataTransaction { persistJob(job); persistInput(job, input) }.mapResult { job }
}

private suspend inline fun <INPUT> ApplicationCall.validateAndSubmit(
    apiConfig: ApiConfig<INPUT, *>,
    jobConfig: JobConfig<INPUT, *>,
    input: INPUT
): String? {
    val inputValidation = apiConfig.inputValidation(input)
    if (!inputValidation.success) {
        respond(inputValidation.responseCode, inputValidation.message)
        return null
    }
    return submit(input, jobConfig).orQuitWith {
        respondText("Failed to persist job: $it", status = InternalServerError)
        return null
    }.uuid
}

private suspend inline fun ApplicationCall.fetchJobAndCheckType(
    uuid: UUID?,
    requestedJobType: String,
    persistence: DataPersistence<*, *>
): Job? {
    val job = persistence.fetchJob(uuid.toString()).orQuitWith {
        when (it) {
            is PersistenceAccessError.InternalError                                    -> respondText(
                "Failed to access job with ID $uuid: $it",
                status = InternalServerError
            )

            is PersistenceAccessError.NotFound, is PersistenceAccessError.UuidNotFound ->
                respondText("No job with ID $uuid could be found.", status = NotFound)

            is PersistenceAccessError.Modified                                         -> error("Unexpected")
        }
        return null
    }
    if (job.type != requestedJobType) {
        respond(BadRequest, "Illegal job type. A job with the given uuid was found, but it was created from a different resource.")
        return null
    }
    return job
}

private suspend inline fun ApplicationCall.resultStatus(uuid: UUID, requestedJobType: String, persistence: DataPersistence<*, *>) {
    fetchJobAndCheckType(uuid, requestedJobType, persistence)?.let { respondText(it.status.toString()) }
}

private suspend inline fun <RESULT> ApplicationCall.result(uuid: String, apiConfig: ApiConfig<*, RESULT>, jobConfig: JobConfig<*, RESULT>) {
    val result = jobConfig.persistence.fetchResult(uuid).orQuitWith {
        respondText("Failed to retrieve job result for ID ${uuid}: $it", status = InternalServerError)
        return
    }
    with(apiConfig) { resultResponder(result) }
    deleteJob(uuid, apiConfig, jobConfig)
}

private suspend inline fun <RESULT> ApplicationCall.failure(
    uuid: String, apiConfig: ApiConfig<*, RESULT>, jobConfig: JobConfig<*, RESULT>, status: HttpStatusCode
) {
    val failure = jobConfig.persistence.fetchFailure(uuid).orQuitWith {
        respondText("Failed to retrieve job failure for ID ${uuid}: $it", status = InternalServerError)
        return
    }
    respond(status, failure)
    deleteJob(uuid, apiConfig, jobConfig)
}

private suspend inline fun <RESULT> deleteJob(uuid: String, apiConfig: ApiConfig<*, RESULT>, jobConfig: JobConfig<*, RESULT>) {
    if (apiConfig.deleteJobAfterFetchingResult) {
        jobConfig.persistence.transaction { deleteForUuid(uuid, mapOf(jobConfig.jobType to jobConfig.persistence)) }
            .ifError { apiLog.error("Failed to delete job after fetching result: $it") }

    }
}

private suspend inline fun ApplicationCall.parseUuid(): UUID? = parameters["uuid"]?.let {
    runCatching { UUID.fromString(it) }.getOrNull()
} ?: run {
    respondText("The given uuid ${parameters["uuid"]} has a wrong format.", status = BadRequest)
    return null
}
