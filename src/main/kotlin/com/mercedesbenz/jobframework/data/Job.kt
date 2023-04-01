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
    fun data(): T
    fun serializedData(): ByteArray
}

abstract class SimpleJsonInput<T>(val data: T) : JobInput<T> {
    override fun data(): T = data
    override fun serializedData(): ByteArray = jacksonObjectMapper().writeValueAsBytes(data)
}

interface JobResult<T> {
    val uuid: String
    fun isSuccess(): Boolean
    fun result(): T?
    fun serializedResult(): ByteArray?
    fun error(): String?
}

open class SimpleJsonResult<T>(override val uuid: String, val result: T?, val error: String?) : JobResult<T> {
    override fun isSuccess() = result != null
    override fun result(): T? = result
    override fun serializedResult(): ByteArray? = result?.let { jacksonObjectMapper().writeValueAsBytes(it) }
    override fun error(): String? = error
}

enum class JobStatus {
    CREATED, RUNNING, SUCCESS, FAILURE
}
