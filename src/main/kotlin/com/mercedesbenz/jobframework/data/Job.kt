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

abstract class JobInput<T>(val uuid: String) {
    abstract fun data(): T
    abstract fun serializedData(): ByteArray
    abstract fun validate(): List<String>?
}

abstract class SimpleJsonInput<T>(uuid: String, val data: T) : JobInput<T>(uuid) {
    override fun data(): T = data
    override fun serializedData(): ByteArray = jacksonObjectMapper().writeValueAsBytes(data)
}

abstract class JobResult<T>(val uuid: String) {
    abstract val isSuccess: Boolean
    abstract fun result(): T?
    abstract fun serializedResult(): String?
    abstract fun error(): String?
}

open class SimpleJsonResult<T>(uuid: String, val result: T?, val error: String?) : JobResult<T>(uuid) {
    override val isSuccess = result != null
    override fun result(): T? = result
    override fun serializedResult(): String? = jacksonObjectMapper().writeValueAsString(result)
    override fun error(): String? = error
}

enum class JobStatus {
    CREATED, RUNNING, SUCCESS, FAILURE
}
