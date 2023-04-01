package com.mercedesbenz.jobframework.control

import com.mercedesbenz.jobframework.boundary.Persistence
import com.mercedesbenz.jobframework.data.Job
import com.mercedesbenz.jobframework.data.JobInput
import com.mercedesbenz.jobframework.data.JobResult
import com.mercedesbenz.jobframework.data.JobStatus
import com.mercedesbenz.jobframework.data.PersistenceAccessError
import com.mercedesbenz.jobframework.data.orQuitWith
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.call
import io.ktor.server.request.receiveText
import io.ktor.server.response.respond
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.util.pipeline.PipelineContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime
import java.util.*

val logger: Logger = LoggerFactory.getLogger("JobRouting") // TODO better name

fun <INPUT, RESULT, IN : JobInput<in INPUT>, RES : JobResult<out RESULT>> Route.jobRoutings(
    persistence: Persistence<INPUT, RESULT, IN, RES>,
    jobInputGen: (String) -> IN,
    myInstanceName: String,
    inputValidation: (IN) -> List<String> = { emptyList() },
    tagProvider: (IN) -> List<String> = { emptyList() },
    customInfoProvider: (IN) -> String = { "" },
    priorityProvider: (IN) -> Int = { 0 },
) {
    post("submit") {
        val plainInput = call.receiveText() // TODO we might want to make this call generic, s.t. the library user can decide how the call receives input
        val input = jobInputGen(plainInput)
        submit(input, persistence, myInstanceName, inputValidation, tagProvider, customInfoProvider, priorityProvider)
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
                result.serializedResult()?.let {
                    call.respondText(it.toString(StandardCharsets.UTF_8))
                } ?: run {
                    call.respondText("Expected the JobResult for ID $uuid to have a result.", status = HttpStatusCode.InternalServerError)
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
                result.error()?.let {
                    call.respondText(it)
                } ?: run {
                    call.respondText("Expected the JobResult for ID $uuid to have an error.", status = HttpStatusCode.InternalServerError)
                }
            }
        }
    }

    post("cancel/{uuid}") {
        call.respond(HttpStatusCode.BadRequest, "Sorry, cancellation is not yet implemented")
//        parseUuid()?.let { uuid ->
//            val job = fetchJob(uuid, persistence) ?: return@post
//            when (job.status) {
//                JobStatus.CREATED, JobStatus.RUNNING -> {
//                    withContext(CoroutineName(uuid.toString())) { cancel("User requested cancellation") }
//                    job.status = JobStatus.CANCELLED
//                    persistence.transaction { updateJob(job) }
//                    call.respond("Job with id $uuid was cancelled successfully")
//                }
//                JobStatus.SUCCESS, JobStatus.FAILURE, JobStatus.CANCELLED -> call.respond("Job with id $uuid has already finished with status ${job.status}")
//            }
//        }
    }
}

private suspend inline fun <INPUT, IN : JobInput<in INPUT>> PipelineContext<Unit, ApplicationCall>.submit(
    input: IN,
    persistence: Persistence<INPUT, *, IN, *>,
    myInstanceName: String,
    inputValidation: (IN) -> List<String>,
    tagProvider: (IN) -> List<String>,
    customInfoProvider: (IN) -> String,
    priorityProvider: (IN) -> Int
) {
    inputValidation(input).takeIf { it.isNotEmpty() }?.let {
        call.respond(HttpStatusCode.BadRequest, it.joinToString(", "))
        return
    }
    val uuid = UUID.randomUUID().toString()
    val tags = tagProvider(input)
    val customInfo = customInfoProvider(input)
    val job = Job(uuid, tags, customInfo, priorityProvider(input), myInstanceName, LocalDateTime.now(), JobStatus.CREATED)
    persistence.transaction {
        persistJob(job)
        persistInput(job, input)
    }.orQuitWith {
        call.respond(HttpStatusCode.InternalServerError, "Failed to persist job: $it")
        return
    }
    call.respond(HttpStatusCode.OK, uuid)
}

private suspend fun PipelineContext<Unit, ApplicationCall>.fetchJob(uuid: UUID?, persistence: Persistence<*, *, *, *>): Job? {
    return persistence.fetchJob(uuid.toString()).orQuitWith {
        when (it) {
            is PersistenceAccessError.InternalError -> call.respondText("Failed to access job with ID $uuid: $it", status = HttpStatusCode.InternalServerError)
            is PersistenceAccessError.NotFound -> call.respondText("No job with ID $uuid could be found.", status = HttpStatusCode.NotFound)
        }
        return null
    }
}

private suspend fun PipelineContext<Unit, ApplicationCall>.status(uuid: UUID, persistence: Persistence<*, *, *, *>) {
    fetchJob(uuid, persistence)?.let { call.respond(it.status) }
}

private suspend inline fun <RESULT, RES : JobResult<out RESULT>> PipelineContext<Unit, ApplicationCall>.result(
    job: Job,
    persistence: Persistence<*, RESULT, *, RES>
): RES? {
    return persistence.fetchResult(job.uuid).orQuitWith {
        call.respondText("Failed to retrieve job result for ID ${job.uuid}: $it")
        return null
    }
}

private suspend fun PipelineContext<Unit, ApplicationCall>.parseUuid(): UUID? {
    val plainUuid = call.parameters["uuid"]
    plainUuid?.let {
        try {
            return UUID.fromString(it)
        } catch (@Suppress("SwallowedException") e: IllegalArgumentException) {
            null
        }
    } ?: run {
        call.respondText("The given uuid $plainUuid has a wrong format.", status = HttpStatusCode.BadRequest)
        return null
    }
}
