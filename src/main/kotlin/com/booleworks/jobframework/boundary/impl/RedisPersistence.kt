// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.jobframework.boundary.impl

import com.booleworks.jobframework.boundary.Persistence
import com.booleworks.jobframework.boundary.TransactionalPersistence
import com.booleworks.jobframework.control.unreachable
import com.booleworks.jobframework.data.Job
import com.booleworks.jobframework.data.JobResult
import com.booleworks.jobframework.data.JobStatus
import com.booleworks.jobframework.data.PersistenceAccessResult
import com.booleworks.jobframework.data.internalError
import com.booleworks.jobframework.data.mapResult
import com.booleworks.jobframework.data.result
import com.booleworks.jobframework.data.success
import com.booleworks.jobframework.data.successful
import com.booleworks.jobframework.util.Either
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.Transaction
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

private val logger = LoggerFactory.getLogger(RedisPersistence::class.java)

/**
 * [Persistence] implementation for redis. It requires a [JedisPool] providing the connection to
 * the Redis instance, serializers and deserializers for job inputs and results, and a
 * [Redis configuration][config].
 */
// TODO error handling
open class RedisPersistence<INPUT, RESULT>(
    private val pool: JedisPool,
    private val inputSerializer: (INPUT) -> ByteArray,
    private val resultSerializer: (JobResult<RESULT>) -> ByteArray,
    private val inputDeserializer: (ByteArray) -> INPUT,
    private val resultDeserializer: (ByteArray) -> JobResult<RESULT>,
    private val config: RedisConfig = DefaultRedisConfig(),
) :
    Persistence<INPUT, RESULT> {

    override suspend fun transaction(block: suspend TransactionalPersistence<INPUT, RESULT>.() -> Unit): PersistenceAccessResult<Unit> =
        pool.resource.use { jedis ->
            jedis.multi().run {
                runCatching {
                    RedisTransactionalPersistence(this@run, inputSerializer, resultSerializer, config)
                        .run { block() }
                        .also { exec() }
                }.onFailure {
                    val message = it.message ?: "Undefined error"
                    logger.error("Jedis transaction failed with: $message", it)
                    val discardResult = discard()
                    logger.error("Discarded the transaction with result: $discardResult")
                    return PersistenceAccessResult.internalError(message)
                }
            }
            PersistenceAccessResult.success
        }

    override suspend fun fetchJob(uuid: String): PersistenceAccessResult<Job> =
        pool.resource.use { it.hgetAll(config.jobKey(uuid)) }.redisMapToJob(uuid)

    override suspend fun fetchInput(uuid: String): PersistenceAccessResult<INPUT> {
        val inputBytes = pool.resource.use { it.get(config.inputKey(uuid).toByteArray()) }
        return PersistenceAccessResult.result(inputDeserializer(inputBytes))
    }

    override suspend fun fetchResult(uuid: String): PersistenceAccessResult<JobResult<RESULT>> {
        val resultBytes = pool.resource.use { it.get(config.resultKey(uuid).toByteArray()) }
        return PersistenceAccessResult.result(resultDeserializer(resultBytes))
    }

    override suspend fun allJobsWithStatus(status: JobStatus): PersistenceAccessResult<List<Job>> =
        getAllJobsBy { jedis, key -> jedis.hget(key, "status") == status.toString() }

    override suspend fun allJobsOfInstance(status: JobStatus, instance: String): PersistenceAccessResult<List<Job>> =
        getAllJobsBy { jedis, key -> jedis.hmget(key, "status", "executingInstance") == listOf(status.toString(), instance) }

    override suspend fun allJobsFinishedBefore(date: LocalDateTime): PersistenceAccessResult<List<Job>> = getAllJobsBy { jedis, key ->
        val statusAndFinishedAt = jedis.hmget(key, "status", "finishedAt")
        (statusAndFinishedAt[0] == JobStatus.SUCCESS.toString() || statusAndFinishedAt[0] == JobStatus.FAILURE.toString())
                && statusAndFinishedAt.getOrNull(1)?.let { LocalDateTime.parse(it) }?.isBefore(date) ?: false
    }

    private fun getAllJobsBy(condition: (Jedis, String) -> Boolean): PersistenceAccessResult<List<Job>> {
        val jobResults = pool.resource.use { jedis ->
            jedis.keys(config.jobPattern)
                .filter { condition(jedis, it) }
                .map { jedis.hgetAll(it).redisMapToJob(config.extractUuid(it)) }
        }
        // TODO complicated
        return jobResults.find { !it.successful }?.mapResult { unreachable() } ?: PersistenceAccessResult.result(jobResults.map { (it as Either.Right).value })
    }
}

/**
 * [TransactionalPersistence] implementation for Jedis.
 *
 * Note that all methods will always return [PersistenceAccessResult.success] since commands within transaction are not yet
 * executed, so the only real kind of error would be connection problems which are ok to be caught in [RedisPersistence.transaction].
 */
open class RedisTransactionalPersistence<INPUT, RESULT>(
    private val transaction: Transaction,
    private val inputSerializer: (INPUT) -> ByteArray,
    private val resultSerializer: (JobResult<RESULT>) -> ByteArray,
    private val config: RedisConfig
) :
    TransactionalPersistence<INPUT, RESULT> {

    override suspend fun persistJob(job: Job): PersistenceAccessResult<Unit> {
        transaction.hset(config.jobKey(job.uuid), job.toRedisMap())
        return PersistenceAccessResult.success
    }

    override suspend fun persistInput(job: Job, input: INPUT): PersistenceAccessResult<Unit> {
        transaction.set(config.inputKey(job.uuid).toByteArray(), inputSerializer(input))
        kotlin.runCatching { }
        return PersistenceAccessResult.success
    }

    override suspend fun persistOrUpdateResult(job: Job, result: JobResult<RESULT>): PersistenceAccessResult<Unit> {
        transaction.set(config.resultKey(job.uuid).toByteArray(), resultSerializer(result))
        return PersistenceAccessResult.success
    }

    override suspend fun updateJob(job: Job): PersistenceAccessResult<Unit> = persistJob(job)

    override suspend fun deleteForUuid(uuid: String): PersistenceAccessResult<Unit> {
        transaction.del(config.jobKey(uuid))
        transaction.del(config.inputKey(uuid))
        transaction.del(config.resultKey(uuid))
        return PersistenceAccessResult.success
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

internal fun Map<String, String>.redisMapToJob(uuidIn: String? = null): PersistenceAccessResult<Job> = try {
    val uuid = uuidIn ?: this["uuid"] ?: run { return PersistenceAccessResult.internalError("Could not find field 'uuid'") }
    PersistenceAccessResult.result(
        Job(
            uuid,
            this["tags"]?.split(TAG_SEPARATOR) ?: emptyList(),
            this["customInfo"],
            this["priority"]?.toIntOrNull() ?: run { return PersistenceAccessResult.internalError("Could not find or convert field 'priority' for ID $uuid") },
            this["createdBy"] ?: run { return PersistenceAccessResult.internalError("Could not find field 'createdBy' for ID $uuid") },
            this["createdAt"]?.let { LocalDateTime.parse(it) }
                ?: run { return PersistenceAccessResult.internalError("Could not find field 'createdAt' for ID $uuid") },
            this["status"]?.let { JobStatus.valueOf(it) }
                ?: run { return PersistenceAccessResult.internalError("Could not find or convert field 'status' for ID $uuid") },
            this["startedAt"]?.let { LocalDateTime.parse(it) },
            this["executingInstance"],
            this["finishedAt"]?.let { LocalDateTime.parse(it) },
            this["timeout"]?.let { LocalDateTime.parse(it) },
            this["numRestarts"]?.toIntOrNull()
                ?: run { return PersistenceAccessResult.internalError("Could not find field or convert field 'numRestarts' for ID $uuid") },
        )
    )
} catch (e: Exception) {
    val message = "Could not read job due to: ${e.message}"
    logger.error(message, e)
    PersistenceAccessResult.internalError(message)
}

private const val TAG_SEPARATOR = """\\\\"""
