// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.api.impl

import com.booleworks.kjobs.api.DataPersistence
import com.booleworks.kjobs.api.DataTransactionalPersistence
import com.booleworks.kjobs.api.JobPersistence
import com.booleworks.kjobs.api.JobTransactionalPersistence
import com.booleworks.kjobs.common.Either
import com.booleworks.kjobs.data.Heartbeat
import com.booleworks.kjobs.data.Job
import com.booleworks.kjobs.data.JobResult
import com.booleworks.kjobs.data.JobStatus
import com.booleworks.kjobs.data.PersistenceAccessError
import com.booleworks.kjobs.data.PersistenceAccessResult
import com.booleworks.kjobs.data.internalError
import com.booleworks.kjobs.data.notFound
import com.booleworks.kjobs.data.result
import com.booleworks.kjobs.data.success
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.Transaction
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

private val logger = LoggerFactory.getLogger(RedisDataPersistence::class.java)

/**
 * [DataPersistence] implementation for Redis. It requires a [JedisPool] providing the connection to
 * the Redis instance, serializers and deserializers for job inputs and results, and a
 * [Redis configuration][config].
 */
open class RedisDataPersistence<INPUT, RESULT>(
    private val pool: JedisPool,
    private val inputSerializer: (INPUT) -> ByteArray,
    private val resultSerializer: (JobResult<RESULT>) -> ByteArray,
    private val inputDeserializer: (ByteArray) -> INPUT,
    private val resultDeserializer: (ByteArray) -> JobResult<RESULT>,
    private val config: RedisConfig = DefaultRedisConfig(),
) : RedisJobPersistence(pool, config), DataPersistence<INPUT, RESULT> {
    override suspend fun transaction(block: suspend JobTransactionalPersistence.() -> Unit): PersistenceAccessResult<Unit> = dataTransaction(block)

    override suspend fun dataTransaction(block: suspend DataTransactionalPersistence<INPUT, RESULT>.() -> Unit): PersistenceAccessResult<Unit> =
        pool.resource.use { jedis ->
            jedis.multi().run {
                runCatching {
                    RedisDataTransactionalPersistence(this@run, inputSerializer, resultSerializer, config)
                        .run { block() }
                        .also { exec().filterIsInstance<Throwable>().firstOrNull()?.let { handleTransactionException(it) } }
                }.onFailure {
                    return handleTransactionException(it)
                }
            }
            PersistenceAccessResult.success
        }

    override suspend fun fetchInput(uuid: String): PersistenceAccessResult<INPUT> {
        val inputBytes = pool.resource.use { it.get(config.inputKey(uuid).toByteArray()) } ?: run { return PersistenceAccessResult.notFound() }
        return PersistenceAccessResult.result(inputDeserializer(inputBytes))
    }

    override suspend fun fetchResult(uuid: String): PersistenceAccessResult<JobResult<RESULT>> {
        val resultBytes = pool.resource.use { it.get(config.resultKey(uuid).toByteArray()) } ?: run { return PersistenceAccessResult.notFound() }
        return PersistenceAccessResult.result(resultDeserializer(resultBytes))
    }

    private fun Transaction.handleTransactionException(ex: Throwable): PersistenceAccessResult<Unit> {
        val message = ex.message ?: "Undefined error"
        logger.error("Jedis transaction failed with: $message", ex)
        val discardResult = discard()
        logger.error("Discarded the transaction with result: $discardResult")
        return PersistenceAccessResult.internalError(message)
    }
}

/**
 * [DataTransactionalPersistence] implementation for Redis.
 *
 * Note that all methods will always return `PersistenceAccessResult.success`, since commands within transaction are not yet
 * executed, so the only real kind of error would be connection problems which are ok to be caught in [RedisDataPersistence.dataTransaction].
 */
open class RedisDataTransactionalPersistence<INPUT, RESULT>(
    private val transaction: Transaction,
    private val inputSerializer: (INPUT) -> ByteArray,
    private val resultSerializer: (JobResult<RESULT>) -> ByteArray,
    private val config: RedisConfig
) : RedisJobTransactionalPersistence(transaction, config), DataTransactionalPersistence<INPUT, RESULT> {

    override suspend fun persistInput(job: Job, input: INPUT): PersistenceAccessResult<Unit> {
        transaction.set(config.inputKey(job.uuid).toByteArray(), inputSerializer(input))
        return PersistenceAccessResult.success
    }

    override suspend fun persistOrUpdateResult(job: Job, result: JobResult<RESULT>): PersistenceAccessResult<Unit> {
        transaction.set(config.resultKey(job.uuid).toByteArray(), resultSerializer(result))
        return PersistenceAccessResult.success
    }
}

/**
 * [JobPersistence] implementation for Redis. It requires a [JedisPool] providing the connection to
 * the Redis instance and a [Redis configuration][config].
 */
open class RedisJobPersistence(
    private val pool: JedisPool,
    private val config: RedisConfig = DefaultRedisConfig(),
) : JobPersistence {
    override suspend fun transaction(block: suspend JobTransactionalPersistence.() -> Unit): PersistenceAccessResult<Unit> = pool.resource.use { jedis ->
        jedis.multi().run {
            runCatching {
                RedisJobTransactionalPersistence(this@run, config)
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

    override suspend fun fetchHeartBeats(since: LocalDateTime): PersistenceAccessResult<List<Heartbeat>> {
        val heartbeatKeys = pool.resource.use { it.keys(config.heartbeatPattern) }.toTypedArray()
        val plainHeartbeats = pool.resource.use { it.mget(*heartbeatKeys) } ?: run { return PersistenceAccessResult.notFound() }
        val filteredHeartbeats = plainHeartbeats.mapIndexed { index, beat ->
            Heartbeat(config.extractInstanceName(heartbeatKeys[index]), LocalDateTime.parse(beat))
        }.filter { it.lastBeat.isAfter(since) }
        return PersistenceAccessResult.result(filteredHeartbeats)
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
        jobResults.filterIsInstance<Either.Left<PersistenceAccessError>>().firstOrNull()?.let { return@getAllJobsBy it }
        return jobResults.map { (it as Either.Right).value }.let { PersistenceAccessResult.result(it) }
    }
}

/**
 * [JobTransactionalPersistence] implementation for Redis.
 *
 * Note that all methods will always return `PersistenceAccessResult.success`, since commands within transaction are not yet
 * executed, so the only real kind of error would be connection problems which are ok to be caught in [RedisJobPersistence.transaction].
 */
open class RedisJobTransactionalPersistence(
    private val transaction: Transaction,
    private val config: RedisConfig
) : JobTransactionalPersistence {
    override suspend fun persistJob(job: Job): PersistenceAccessResult<Unit> {
        transaction.hset(config.jobKey(job.uuid), job.toRedisMap())
        return PersistenceAccessResult.success
    }

    override suspend fun updateJob(job: Job): PersistenceAccessResult<Unit> {
        persistJob(job)
        transaction.hdel(config.jobKey(job.uuid), *job.nullFields().toTypedArray())
        return PersistenceAccessResult.success
    }

    override suspend fun deleteForUuid(uuid: String): PersistenceAccessResult<Unit> {
        transaction.del(config.jobKey(uuid))
        transaction.del(config.inputKey(uuid))
        transaction.del(config.resultKey(uuid))
        return PersistenceAccessResult.success
    }

    override suspend fun updateHeartbeat(heartbeat: Heartbeat): PersistenceAccessResult<Unit> {
        transaction.set(config.heartbeatKey(heartbeat.instanceName), heartbeat.lastBeat.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
        return PersistenceAccessResult.success
    }
}

internal fun Job.toRedisMap(): Map<String, String> =
    mandatoryFields() + optionalFields().filter { it.second != null }.associate { it.first to it.second as String }

internal fun Job.nullFields(): List<String> = optionalFields().filter { it.second == null }.map { it.first }

private fun Job.mandatoryFields() = mapOf(
    "type" to type,
    "priority" to priority.toString(),
    "createdBy" to createdBy,
    "createdAt" to createdAt.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
    "status" to status.toString(),
    "numRestarts" to numRestarts.toString(),
)

private fun Job.optionalFields() = listOf(
    "tags" to tags.takeIf { it.isNotEmpty() }?.joinToString(TAG_SEPARATOR),
    "customInfo" to customInfo,
    "startedAt" to startedAt?.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
    "executingInstance" to executingInstance,
    "finishedAt" to finishedAt?.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
    "timeout" to timeout?.toString()
)

internal fun Map<String, String>.redisMapToJob(uuidIn: String? = null): PersistenceAccessResult<Job> = try {
    val uuid = uuidIn ?: this["uuid"] ?: run { return PersistenceAccessResult.internalError("Could not find field 'uuid'") }
    PersistenceAccessResult.result(
        Job(
            uuid,
            this["type"] ?: run { return PersistenceAccessResult.internalError("Could not find field 'type' for ID $uuid") },
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
