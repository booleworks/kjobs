package com.mercedesbenz.jobframework.data

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.time.LocalDateTime

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

interface JobInput<T> {
    val uuid: String
    fun data(): T
    fun serializedData(): ByteArray
    fun validate(): List<String>?
}

abstract class SimpleJsonInput<T>(override val uuid: String, val data: T) : JobInput<T> {
    override fun data(): T = data
    override fun serializedData(): ByteArray = jacksonObjectMapper().writeValueAsBytes(data)
}

interface JobResult<T> {
    val uuid: String
    val isSuccess: Boolean
    fun result(): T?
    fun serializedResult(): String?
    fun error(): String?
}

open class SimpleJsonResult<T>(override val uuid: String, val result: T?, val error: String?) : JobResult<T> {
    override val isSuccess = result != null
    override fun result(): T? = result
    override fun serializedResult(): String? = jacksonObjectMapper().writeValueAsString(result)
    override fun error(): String? = error
}

enum class JobStatus {
    CREATED, RUNNING, SUCCESS, FAILURE
}
