// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.control

import com.booleworks.kjobs.api.JobFrameworkBuilder
import com.booleworks.kjobs.api.SynchronousResourceConfigBuilder
import com.booleworks.kjobs.api.persistence.DataPersistence
import com.booleworks.kjobs.data.Job
import com.booleworks.kjobs.data.JobStatus
import com.booleworks.kjobs.data.PersistenceAccessError
import com.booleworks.kjobs.data.PersistenceAccessResult
import com.booleworks.kjobs.data.mapResult
import com.booleworks.kjobs.data.orQuitWith
import io.ktor.http.HttpStatusCode.Companion.BadRequest
import io.ktor.http.HttpStatusCode.Companion.InternalServerError
import io.ktor.http.HttpStatusCode.Companion.NotFound
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.call
import io.ktor.server.response.respond
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.delete
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.route
import io.ktor.util.pipeline.PipelineContext
import kotlinx.coroutines.delay
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.util.*
import kotlin.time.Duration

private val apiLog = LoggerFactory.getLogger("ApiLog")

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
 * - if no such job exists or it is not in status `SUCCESS`, status 404 is returned
 *
 * ### `GET failure/{uuid}`
 * - returns the failure of the job with the given uuid
 * - if no such job exists or it is not in status `FAILURE`, status 404 is returned
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
 * ### `POST synchronous` (if enabled via [SynchronousResourceConfigBuilder.enabled])
 * - performs the same steps as `submit` but does not return the UUID
 * - waits until the job is computed (computation might be performed by another instance)
 * - returns the result of the computation using [ApiConfig.resultResponder]
 *
 * ### `DELETE delete/{uuid}` (if enabled via [ApiConfig.enableDeletion])
 * - deletes the job with the given uuid
 * - the job must have the `jobType` of this API, otherwise status 400 is returned. So only jobs belonging
 *     to this API can be deleted.
 * - the job must not be in one of the stati `CREATED`, `RUNNING`, or `CANCEL_REQUESTED`. Such jobs must be
 *     canceled first (and/or it has to be waited until the cancellation has succeeded and the status is
 *     `CANCELLED`)
 *
 * If the persistence access in any of the routes fails, status 500 with a respective message is returned.
 */
internal fun <INPUT, RESULT> Route.setupJobApi(apiConfig: ApiConfig<INPUT, RESULT>, jobConfig: JobConfig<INPUT, RESULT>) = with(apiConfig) {
    route(apiConfig.basePath ?: "") {
        post("submit") {
            validateAndSubmit(apiConfig, jobConfig, inputReceiver())?.let { call.respond(it) }
        }

        get("status/{uuid}") {
            parseUuid()?.let { resultStatus(it, jobConfig.persistence) }
        }

        get("result/{uuid}") {
            parseUuid()?.let { uuid ->
                val job = fetchJob(uuid, jobConfig.persistence) ?: return@get
                if (job.status != JobStatus.SUCCESS) {
                    call.respondText("Cannot return the result for job with ID $uuid and status ${job.status}.", status = BadRequest)
                } else {
                    result(job, jobConfig.persistence)?.let { resultResponder(it) }
                }
            }
        }

        get("failure/{uuid}") {
            parseUuid()?.let { uuid ->
                val job = fetchJob(uuid, jobConfig.persistence) ?: return@get
                if (job.status != JobStatus.FAILURE) {
                    call.respondText("Cannot return a failure for job with ID $uuid and status ${job.status}.", status = BadRequest)
                } else {
                    failure(job, jobConfig.persistence)?.let { call.respondText(it) }
                }
            }
        }

        if (enableDeletion) {
            delete("delete/{uuid}") {
                parseUuid()?.let { uuid ->
                    fetchJob(uuid, jobConfig.persistence)?.let { job ->
                        if (job.type != jobConfig.jobType) {
                            call.respond(
                                BadRequest,
                                "The job with ID $uuid belongs to another job type. Please call the delete resource with the correct path."
                            )
                        } else if (job.status in setOf(JobStatus.CREATED, JobStatus.RUNNING, JobStatus.CANCEL_REQUESTED)) {
                            call.respond(BadRequest, "Cannot delete job with ID $uuid, because it is in a wrong status: ${job.status}")
                        } else {
                            jobConfig.persistence.transaction { deleteForUuid(uuid.toString(), mapOf(jobConfig.jobType to jobConfig.persistence)) }
                                .onLeft { call.respond(InternalServerError, "Deletion of Job with ID $uuid failed with: $it") }
                                .onRight { call.respond("Deleted") }
                        }
                    }
                }
            }
        }

        if (enableCancellation) {
            post("cancel/{uuid}") {
                parseUuid()?.let { uuid ->
                    val job = fetchJob(uuid, jobConfig.persistence) ?: return@post
                    call.respond(cancelJob(job, jobConfig.persistence))
                }
            }
        }

        if (syncMockConfig.enabled) {
            post(syncMockConfig.path) {
                val uuid = validateAndSubmit(apiConfig, jobConfig.copy(priorityProvider = syncMockConfig.priorityProvider), inputReceiver()) ?: return@post
                apiLog.trace("Submitted job for synchronous computation, got ID $uuid")
                var status: JobStatus
                val timeout = LocalDateTime.now().plusSeconds(syncMockConfig.maxWaitingTime.inWholeSeconds)
                do {
                    delay(syncMockConfig.checkInterval)
                    status = jobConfig.persistence.fetchJob(uuid).orQuitWith {
                        call.respondText("Synchronous computation failed during job access: ${it.message}", status = InternalServerError)
                        return@post
                    }.status
                    apiLog.trace("Synchronous job with ID {} is in status {}", uuid, status)
                } while (status in setOf(JobStatus.CREATED, JobStatus.RUNNING) && LocalDateTime.now() < timeout)
                if (status == JobStatus.SUCCESS) {
                    jobConfig.persistence.fetchResult(uuid)
                        .orQuitWith { call.respond(InternalServerError, "Failed to retrieve job result"); return@post }
                        .let { resultResponder(it) }
                } else if (status == JobStatus.FAILURE) {
                    jobConfig.persistence.fetchFailure(uuid)
                        .orQuitWith { call.respond(InternalServerError, "Failed to retrieve job result"); return@post }
                        .let { call.respond(InternalServerError, "Computation failed with message: $it") }
                } else {
                    if (LocalDateTime.now() > timeout) {
                        call.respondText(
                            "The job did not finish within the timeout of ${syncMockConfig.maxWaitingTime}. " +
                                    "You may be able to retrieve the result later via the asynchronous API using the job id $uuid.", status = BadRequest
                        )
                    } else {
                        call.respondText("Job finished in an unexpected status $status", status = InternalServerError)
                    }
                }
            }
        }
    }
}

internal suspend inline fun <INPUT, RESULT> cancelJob(job: Job, persistence: DataPersistence<INPUT, RESULT>): String = when (job.status) {
    // TODO: what about the risk of the job status being overridden?
    //  The execution job could take the job at the same time and might not see the respective cancellation update.
    JobStatus.CREATED -> {
        apiLog.info("Cancelling job with ID ${job.uuid} from status CREATED")
        job.status = JobStatus.CANCELLED
        job.finishedAt = LocalDateTime.now()
        persistence.transaction { updateJob(job) }
        "Job with id ${job.uuid} was cancelled successfully"
    }

    JobStatus.RUNNING -> {
        apiLog.info("Cancelling job with ID ${job.uuid} from status RUNNING")
        job.status = JobStatus.CANCEL_REQUESTED
        persistence.transaction { updateJob(job) }
        "Job with id ${job.uuid} is currently running and will be cancelled as soon as possible. " +
                "If it finishes in the meantime, the cancel request will be ignored."
    }

    JobStatus.CANCEL_REQUESTED -> {
        apiLog.warn("Detected a duplicate cancellation request for job with ID ${job.uuid}")
        "Cancellation for job with id ${job.uuid} has already been requested"
    }

    JobStatus.SUCCESS, JobStatus.FAILURE, JobStatus.CANCELLED -> {
        if (job.status == JobStatus.CANCELLED) apiLog.warn("Detected a duplicate cancellation request for job with ID ${job.uuid}")
        else apiLog.warn("Got a cancellation request for already finished job in status ${job.status} with ID ${job.uuid}")
        "Job with id ${job.uuid} has already finished with status ${job.status}"
    }
}

internal class ApiConfig<INPUT, RESULT>(
    val inputReceiver: suspend PipelineContext<Unit, ApplicationCall>.() -> INPUT,
    val resultResponder: suspend PipelineContext<Unit, ApplicationCall>.(RESULT) -> Unit,
    val basePath: String?,
    val inputValidation: (INPUT) -> List<String>,
    val enableDeletion: Boolean,
    val enableCancellation: Boolean,
    val syncMockConfig: SynchronousResourceConfig<INPUT>
)

data class JobConfig<INPUT, RESULT>(
    val jobType: String,
    val persistence: DataPersistence<INPUT, RESULT>,
    val myInstanceName: String,
    val tagProvider: (INPUT) -> List<String>,
    val customInfoProvider: (INPUT) -> String?,
    val priorityProvider: (INPUT) -> Int,
)

internal class SynchronousResourceConfig<INPUT>(
    val enabled: Boolean,
    val path: String,
    val checkInterval: Duration,
    val maxWaitingTime: Duration,
    val priorityProvider: (INPUT) -> Int
)

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

private suspend inline fun <INPUT> PipelineContext<Unit, ApplicationCall>.validateAndSubmit(
    apiConfig: ApiConfig<INPUT, *>,
    jobConfig: JobConfig<INPUT, *>,
    input: INPUT
): String? {
    apiConfig.inputValidation(input).takeIf { it.isNotEmpty() }?.let {
        call.respond(BadRequest, it.joinToString(", "))
        return null
    }
    return submit(input, jobConfig).orQuitWith {
        call.respondText("Failed to persist job: $it", status = InternalServerError)
        return null
    }.uuid
}

private suspend inline fun PipelineContext<Unit, ApplicationCall>.fetchJob(uuid: UUID?, persistence: DataPersistence<*, *>): Job? {
    return persistence.fetchJob(uuid.toString()).orQuitWith {
        when (it) {
            is PersistenceAccessError.InternalError -> call.respondText("Failed to access job with ID $uuid: $it", status = InternalServerError)
            is PersistenceAccessError.NotFound, is PersistenceAccessError.UuidNotFound ->
                call.respondText("No job with ID $uuid could be found.", status = NotFound)
        }
        return null
    }
}

private suspend inline fun PipelineContext<Unit, ApplicationCall>.resultStatus(uuid: UUID, persistence: DataPersistence<*, *>) {
    fetchJob(uuid, persistence)?.let { call.respondText(it.status.toString()) }
}

private suspend inline fun <RESULT> PipelineContext<Unit, ApplicationCall>.result(job: Job, persistence: DataPersistence<*, RESULT>): RESULT? =
    persistence.fetchResult(job.uuid).orQuitWith {
        call.respondText("Failed to retrieve job result for ID ${job.uuid}: $it", status = InternalServerError)
        return null
    }

private suspend inline fun <RESULT> PipelineContext<Unit, ApplicationCall>.failure(job: Job, persistence: DataPersistence<*, RESULT>): String? =
    persistence.fetchFailure(job.uuid).orQuitWith {
        call.respondText("Failed to retrieve job failure for ID ${job.uuid}: $it", status = InternalServerError)
        return null
    }

private suspend inline fun PipelineContext<Unit, ApplicationCall>.parseUuid(): UUID? = call.parameters["uuid"]?.let {
    runCatching { UUID.fromString(it) }.getOrNull()
} ?: run {
    call.respondText("The given uuid ${call.parameters["uuid"]} has a wrong format.", status = BadRequest)
    return null
}
