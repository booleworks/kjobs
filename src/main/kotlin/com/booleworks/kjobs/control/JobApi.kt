// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.control

import com.booleworks.kjobs.api.DataPersistence
import com.booleworks.kjobs.data.Job
import com.booleworks.kjobs.data.JobResult
import com.booleworks.kjobs.data.JobStatus
import com.booleworks.kjobs.data.PersistenceAccessError
import com.booleworks.kjobs.data.orQuitWith
import io.ktor.http.HttpStatusCode
import io.ktor.http.HttpStatusCode.Companion.BadRequest
import io.ktor.http.HttpStatusCode.Companion.InternalServerError
import io.ktor.http.HttpStatusCode.Companion.NotFound
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.call
import io.ktor.server.response.respond
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.route
import io.ktor.util.pipeline.PipelineContext
import kotlinx.coroutines.delay
import java.time.LocalDateTime
import java.util.*
import kotlin.time.Duration

/**
 * Setup all routings according to a given [JobApiDef].
 *
 * The following routes will be created:
 * - `POST submit`
 *     - receives its input using [JobApiDef.inputReceiver]
 *     - optionally validates the input using [JobApiDef.inputValidation]
 *     - creates a new [Job] stores the job and the input in the [persistence][JobApiDef.persistence]
 * - `GET status/{uuid}`
 *     - returns the status of the job with the given uuid
 *     - if no such job exists, status 404 is returned
 * - `GET result/{uuid}`
 *     - returns the result using [JobApiDef.resultResponder]
 *     - if no such job exists or it is not in status `SUCCESS`, status 404 is returned
 * - `GET failure/{uuid}`
 *     - returns the failure of the job with the given uuid
 *     - if no such job exists or it is not in status `FAILURE`, status 404 is returned
 * - `POST cancel/{uuid}` (if enabled via [JobApiDef.enableCancellation])
 *     - cancels the job with the given uuid
 *     - if no such job exists, status 404 is returned
 *     - if the job is in status `CREATED`, it is cancelled immediately
 *     - if the job is in status `CANCEL_REQUESTED`, `CANCELLED`, `SUCCESS`, or `FAILURE`, the request is ignored
 *       with http status 200 and a respective message
 *     - if the job is in status `RUNNING` it will go to `CANCEL_REQUESTED`. As soon as the computation is aborted
 *       the job will change to status `CANCELLED`. However, if the computation finishes (with `SUCCESS` or
 *       `FAILURE`) before it is aborted, the respecting finishing status will be set instead of `CANCELLED`
 *       and the result (or error) will be stored.
 *
 * If the persistence access in any of the routes fails, status 500 with a respective message is returned.
 */
internal fun <INPUT, RESULT> Route.setupJobApi(def: JobApiDef<INPUT, RESULT>) = with(def) {
    route(basePath ?: "") {
        post("submit") {
            submit(def, inputReceiver(), def.priorityProvider)?.let { call.respond(it) }
        }

        get("status/{uuid}") {
            parseUuid()?.let { status(it, persistence) }
        }

        get("result/{uuid}") {
            parseUuid()?.let { uuid ->
                val job = fetchJob(uuid, persistence) ?: return@get
                if (job.status != JobStatus.SUCCESS) {
                    call.respondText("Cannot return the result for job with ID $uuid and status ${job.status}.", status = HttpStatusCode.BadRequest)
                } else {
                    val result = result(job, persistence) ?: return@get
                    result.result?.let {
                        resultResponder(it)
                    } ?: run {
                        call.respondText("Expected the JobResult for ID $uuid to have a result.", status = InternalServerError)
                    }
                }
            }
        }

        get("failure/{uuid}") {
            parseUuid()?.let { uuid ->
                val job = fetchJob(uuid, persistence) ?: return@get
                if (job.status != JobStatus.FAILURE) {
                    call.respondText("Cannot return a failure for job with ID $uuid and status ${job.status}.", status = HttpStatusCode.BadRequest)
                } else {
                    val result = result(job, persistence) ?: return@get
                    result.error?.let {
                        call.respondText(it)
                    } ?: run {
                        call.respondText("Expected the JobResult for ID $uuid to have an error.", status = InternalServerError)
                    }
                }
            }
        }

        if (enableCancellation) {
            post("cancel/{uuid}") {
                parseUuid()?.let { uuid ->
                    val job = fetchJob(uuid, persistence) ?: return@post
                    call.respond(cancelJob(job, uuid, persistence))
                }
            }
        }

        if (syncMockConfig.enabled) {
            post("synchronous") {
                val uuid = submit(def, inputReceiver(), syncMockConfig.priorityProvider) ?: return@post
                var status = JobStatus.CREATED
                val timeout = LocalDateTime.now().plusSeconds(syncMockConfig.maxWaitingTime.inWholeSeconds)
                while (status in setOf(JobStatus.CREATED, JobStatus.RUNNING) && LocalDateTime.now() < timeout) {
                    delay(syncMockConfig.checkInterval)
                    status = persistence.fetchJob(uuid).orQuitWith {
                        call.respondText("Synchronous computation failed during job access: ${it.message}", status = InternalServerError)
                        return@post
                    }.status
                }
                if (status != JobStatus.SUCCESS) {
                    if (LocalDateTime.now() > timeout) {
                        call.respondText(
                            "The job did not finish within the timeout of ${syncMockConfig.maxWaitingTime}. " +
                                    "You may be able to retrieve the result later using the job id $uuid.", status = BadRequest
                        )
                    } else {
                        call.respondText("Job finished in unexpected status $status", status = InternalServerError)
                    }
                } else {
                    val result = persistence.fetchResult(uuid).orQuitWith { call.respond(InternalServerError, "Failed to retrieve job result"); return@post }
                    result.result?.let { resultResponder(it) } ?: run {
                        call.respondText("Expected the JobResult for ID $uuid to have a result.", status = InternalServerError)
                    }
                }
            }
        }
    }
}

internal suspend fun <INPUT, RESULT> cancelJob(job: Job, uuid: UUID, persistence: DataPersistence<INPUT, RESULT>): String = when (job.status) {
    // TODO: what about the risk of the job status being overridden?
    //  The execution job could take the job at the same time and might not see the respective cancellation update.
    JobStatus.CREATED -> {
        job.status = JobStatus.CANCELLED
        job.finishedAt = LocalDateTime.now()
        persistence.transaction { updateJob(job) }
        "Job with id $uuid was cancelled successfully"
    }

    JobStatus.RUNNING -> {
        job.status = JobStatus.CANCEL_REQUESTED
        persistence.transaction { updateJob(job) }
        "Job with id $uuid is currently running and will be cancelled as soon as possible. If it finishes in the meantime, the cancel request will be ignored."
    }

    JobStatus.CANCEL_REQUESTED -> "Cancellation for job with id $uuid has already been requested"
    JobStatus.SUCCESS, JobStatus.FAILURE, JobStatus.CANCELLED ->
        "Job with id $uuid has already finished with status ${job.status}"
}

internal class JobApiDef<INPUT, RESULT>(
    val jobType: String,
    val persistence: DataPersistence<INPUT, RESULT>,
    val myInstanceName: String,
    val inputReceiver: suspend PipelineContext<Unit, ApplicationCall>.() -> INPUT,
    val resultResponder: suspend PipelineContext<Unit, ApplicationCall>.(RESULT) -> Unit,
    val basePath: String?,
    val inputValidation: (INPUT) -> List<String>,
    val tagProvider: (INPUT) -> List<String>,
    val customInfoProvider: (INPUT) -> String,
    val priorityProvider: (INPUT) -> Int,
    val enableCancellation: Boolean,
    val syncMockConfig: SyncMockConfiguration<INPUT>
)

internal class SyncMockConfiguration<INPUT>(
    val enabled: Boolean,
    val checkInterval: Duration,
    val maxWaitingTime: Duration,
    val priorityProvider: (INPUT) -> Int
)

private suspend inline fun <INPUT> PipelineContext<Unit, ApplicationCall>.submit(
    def: JobApiDef<INPUT, *>,
    input: INPUT,
    customPriorityProvider: (INPUT) -> Int
): String? = with(def) {
    inputValidation(input).takeIf { it.isNotEmpty() }?.let {
        call.respond(HttpStatusCode.BadRequest, it.joinToString(", "))
        return null
    }
    val uuid = UUID.randomUUID().toString()
    val tags = tagProvider(input)
    val customInfo = customInfoProvider(input)
    val job = Job(uuid, jobType, tags, customInfo, customPriorityProvider(input), myInstanceName, LocalDateTime.now(), JobStatus.CREATED)
    persistence.dataTransaction {
        persistJob(job)
        persistInput(job, input)
    }.orQuitWith {
        call.respondText("Failed to persist job: $it", status = InternalServerError)
        return null
    }
    uuid
}

private suspend fun PipelineContext<Unit, ApplicationCall>.fetchJob(uuid: UUID?, persistence: DataPersistence<*, *>): Job? {
    return persistence.fetchJob(uuid.toString()).orQuitWith {
        when (it) {
            is PersistenceAccessError.InternalError -> call.respondText("Failed to access job with ID $uuid: $it", status = InternalServerError)
            is PersistenceAccessError.NotFound -> call.respondText("No job with ID $uuid could be found.", status = NotFound)
        }
        return null
    }
}

private suspend fun PipelineContext<Unit, ApplicationCall>.status(uuid: UUID, persistence: DataPersistence<*, *>) {
    fetchJob(uuid, persistence)?.let { call.respondText(it.status.toString()) }
}

private suspend inline fun <RESULT> PipelineContext<Unit, ApplicationCall>.result(job: Job, persistence: DataPersistence<*, RESULT>): JobResult<RESULT>? =
    persistence.fetchResult(job.uuid).orQuitWith {
        call.respondText("Failed to retrieve job result for ID ${job.uuid}: $it", status = InternalServerError)
        return null
    }

private suspend fun PipelineContext<Unit, ApplicationCall>.parseUuid(): UUID? = call.parameters["uuid"]?.let {
    try {
        return UUID.fromString(it)
    } catch (@Suppress("SwallowedException") e: IllegalArgumentException) {
        null
    }
} ?: run {
    call.respondText("The given uuid ${call.parameters["uuid"]} has a wrong format.", status = HttpStatusCode.BadRequest)
    return null
}
