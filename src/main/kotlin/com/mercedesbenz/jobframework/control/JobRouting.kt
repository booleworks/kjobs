package com.mercedesbenz.jobframework.control

import com.mercedesbenz.jobframework.boundary.JobAccessError
import com.mercedesbenz.jobframework.boundary.Persistence
import com.mercedesbenz.jobframework.data.Job
import com.mercedesbenz.jobframework.data.JobInput
import com.mercedesbenz.jobframework.data.JobResult
import com.mercedesbenz.jobframework.data.JobStatus
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.util.pipeline.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.util.*

val logger: Logger = LoggerFactory.getLogger("JobRouting") // TODO better name

fun <INPUT, RESULT, IN : JobInput<INPUT>, RES : JobResult<RESULT>> Route.jobRoutings(
    persistence: Persistence<INPUT, RESULT, IN, RES>,
    jobInputGen: (String) -> IN,
    myInstanceName: String,
    tagProvider: (IN) -> List<String> = { emptyList() },
    customInfoProvider: (IN) -> String = { "" },
    priorityProvider: (IN) -> Int = { 0 },
) {
    post("submit") {
        val plainInput = call.receiveText()
        val input = jobInputGen(plainInput)
        submit(input, persistence, myInstanceName, tagProvider, customInfoProvider, priorityProvider)
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
                    call.respondText(it)
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
}

private suspend inline fun <INPUT, IN : JobInput<INPUT>> PipelineContext<Unit, ApplicationCall>.submit(
    input: IN,
    persistence: Persistence<INPUT, *, IN, *>,
    myInstanceName: String,
    tagProvider: (IN) -> List<String>,
    customInfoProvider: (IN) -> String,
    priorityProvider: (IN) -> Int
) {
    input.validate()?.let {
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
            is JobAccessError.InternalError -> call.respondText("Failed to access job with ID $uuid: $it", status = HttpStatusCode.InternalServerError)
            is JobAccessError.NotFound -> call.respondText("No job with ID $uuid could be found.", status = HttpStatusCode.NotFound)
        }
        return null
    }
}

private suspend fun PipelineContext<Unit, ApplicationCall>.status(uuid: UUID, persistence: Persistence<*, *, *, *>) {
    fetchJob(uuid, persistence)?.let { call.respond(it.status) }
}

private suspend inline fun <RESULT, RES : JobResult<RESULT>> PipelineContext<Unit, ApplicationCall>.result(
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
        } catch (e: IllegalArgumentException) {
            null
        }
    } ?: run {
        call.respondText("The given uuid $plainUuid has a wrong format.", status = HttpStatusCode.BadRequest)
        return null
    }
}
