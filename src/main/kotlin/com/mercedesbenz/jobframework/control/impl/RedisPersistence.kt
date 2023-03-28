package com.mercedesbenz.jobframework.control.impl

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.mercedesbenz.jobframework.boundary.JobAccessResult
import com.mercedesbenz.jobframework.boundary.Persistence
import com.mercedesbenz.jobframework.boundary.TransactionalPersistence
import com.mercedesbenz.jobframework.control.unreachable
import com.mercedesbenz.jobframework.data.Job
import com.mercedesbenz.jobframework.data.JobInput
import com.mercedesbenz.jobframework.data.JobResult
import com.mercedesbenz.jobframework.data.JobStatus
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.Transaction
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

private val logger = LoggerFactory.getLogger(RedisPersistence::class.java)

// TODO error handling
class RedisPersistence<in INPUT, out RESULT, IN : JobInput<in INPUT>, RES : JobResult<out RESULT>>(
    private val pool: JedisPool,
    private val inputDeserializer: (ByteArray) -> IN,
    private val resultDeserializer: (ByteArray) -> RES,
    private val config: RedisConfig,
) :
    Persistence<INPUT, RESULT, IN, RES> {

    override fun transaction(block: TransactionalPersistence<INPUT, RESULT, IN, RES>.() -> Unit): JobAccessResult<Unit> = pool.resource.use { jedis ->
        jedis.multi().run {
            try {
                RedisTransactionalPersistence<INPUT, RESULT, IN, RES>(this@run, config).run(block).also { exec() }
            } catch (e: Throwable) {
                val message = e.message ?: "Undefined error"
                logger.error("Jedis transaction failed with: $message", e)
                val discardResult = discard()
                logger.error("Discarded the transaction with result: $discardResult")
                return JobAccessResult.internalError(message)
            }
        }
        JobAccessResult.success
    }

    override fun fetchJob(uuid: String): JobAccessResult<Job> {
        return pool.resource.use { it.hgetAll(config.jobKey(uuid)) }.redisMapToJob(uuid)
    }

    override fun fetchInput(uuid: String): JobAccessResult<IN> {
        val inputBytes = pool.resource.use { it.get(config.inputKey(uuid).toByteArray()) }
        return JobAccessResult.result(inputDeserializer(inputBytes))
    }

    override fun fetchResult(uuid: String): JobAccessResult<RES> {
        val resultBytes = pool.resource.use { it.get(config.inputKey(uuid).toByteArray()) }
        return JobAccessResult.result(resultDeserializer(resultBytes))
    }

    override fun allJobsFor(status: JobStatus): JobAccessResult<List<Job>> {
        return getAllJobsBy { jedis, key -> jedis.hget(key, "status") == JobStatus.RUNNING.toString() }
    }

    override fun allJobsOfInstance(status: JobStatus, instance: String): JobAccessResult<List<Job>> {
        return getAllJobsBy { jedis, key -> jedis.hmget(key, "status", "executingInstance") == listOf(status.toString(), instance) }
    }

    override fun allJobsFinishedBefore(date: LocalDateTime): JobAccessResult<List<Job>> {
        return getAllJobsBy { jedis, key ->
            val statusAndFinishedAt = jedis.hmget(key, "status", "finishedAt")
            (statusAndFinishedAt[0] == JobStatus.SUCCESS.toString() || statusAndFinishedAt[0] == JobStatus.FAILURE.toString())
                    && statusAndFinishedAt.getOrNull(1)?.let { LocalDateTime.parse(it) }?.isBefore(date) ?: false
        }
    }

    private fun getAllJobsBy(condition: (Jedis, String) -> Boolean): JobAccessResult<List<Job>> {
        val jobResults = pool.resource.use { jedis ->
            jedis.keys(config.jobPattern)
                .filter { condition(jedis, it) }
                .map { jedis.hgetAll(it).redisMapToJob() }
        }
        return jobResults.find { !it.successful }?.mapResult { unreachable() } ?: JobAccessResult.result(jobResults.map { it.result!! })
    }
}

/**
 * [TransactionalPersistence] implementation for Jedis.
 *
 * Note that all methods will always return [JobAccessResult.success] since commands within transaction are not yet
 * executed, so the only real kind of error would be connection problems which are ok to be caught in [RedisPersistence.transaction].
 */
class RedisTransactionalPersistence<in INPUT, out RESULT, IN : JobInput<in INPUT>, RES : JobResult<out RESULT>>(
    private val transaction: Transaction,
    private val config: RedisConfig
) :
    TransactionalPersistence<INPUT, RESULT, IN, RES> {

    override fun persistJob(job: Job): JobAccessResult<Unit> {
        transaction.hset(config.jobKey(job.uuid), job.toRedisMap())
        return JobAccessResult.success
    }

    override fun persistInput(job: Job, input: INPUT): JobAccessResult<Unit> {
        transaction.set(config.inputKey(job.uuid).toByteArray(), jacksonObjectMapper().writeValueAsBytes(input))
        return JobAccessResult.success
    }

    override fun persistResult(job: Job, result: RES): JobAccessResult<Unit> {
        transaction.set(config.resultKey(job.uuid).toByteArray(), jacksonObjectMapper().writeValueAsBytes(result))
        return JobAccessResult.success
    }

    override fun updateJob(job: Job): JobAccessResult<Unit> {
        return persistJob(job)
    }

    override fun deleteForUuid(uuid: String): JobAccessResult<Unit> {
        transaction.del(config.jobKey(uuid))
        transaction.del(config.inputKey(uuid))
        transaction.del(config.resultKey(uuid))
        return JobAccessResult.success
    }
}

internal fun Job.toRedisMap(): Map<String, String> {
    return mapOf(
        "tags" to tags.joinToString(TAG_SEPARATOR),
        "priority" to priority.toString(),
        "createdBy" to createdBy,
        "createdAt" to createdAt.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
        "status" to status.toString(),
        "numRestarts" to numRestarts.toString(),
    ) + listOf(
        "customInfo" to customInfo,
        "startedAt" to startedAt?.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
        "executingInstance" to executingInstance,
        "finishedAt" to finishedAt?.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
        "timeout" to timeout?.toString()
    ).filter { it.second != null }.associate { it.first to it.second as String }
}

internal fun Map<String, String>.redisMapToJob(uuidIn: String? = null): JobAccessResult<Job> = try {
    val uuid = uuidIn ?: this["uuid"] ?: run { return JobAccessResult.internalError("Could not find field 'uuid'") }
    JobAccessResult.result(Job(
        uuid,
        this["tags"]?.split(TAG_SEPARATOR) ?: emptyList(),
        this["customInfo"],
        this["priority"]?.toIntOrNull() ?: run { return JobAccessResult.internalError("Could not find or convert field 'priority' for ID $uuid") },
        this["createdBy"] ?: run { return JobAccessResult.internalError("Could not find field 'createdBy' for ID $uuid") },
        this["createdAt"]?.let { LocalDateTime.parse(it) } ?: run { return JobAccessResult.internalError("Could not find field 'createdAt' for ID $uuid") },
        this["status"]?.let { JobStatus.valueOf(it) } ?: run { return JobAccessResult.internalError("Could not find or convert field 'status' for ID $uuid") },
        this["startedAt"]?.let { LocalDateTime.parse(it) },
        this["executingInstance"],
        this["finishedAt"]?.let { LocalDateTime.parse(it) },
        this["timeout"]?.let { LocalDateTime.parse(it) },
        this["numRestarts"]?.toIntOrNull() ?: run { return JobAccessResult.internalError("Could not find field or convert field 'numRestarts' for ID $uuid") },
    )
    )
} catch (e: Exception) {
    val message = "Could not read job due to: ${e.message}"
    logger.error(message, e)
    JobAccessResult.internalError(message)
}

private const val TAG_SEPARATOR = """\\\\"""
