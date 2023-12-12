// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.api.persistence.redis

import com.booleworks.kjobs.api.persistence.DataPersistence
import com.booleworks.kjobs.api.persistence.DataTransactionalPersistence
import com.booleworks.kjobs.api.persistence.JobPersistence
import com.booleworks.kjobs.api.persistence.JobTransactionalPersistence
import com.booleworks.kjobs.common.Either
import com.booleworks.kjobs.common.unwrapOrReturnFirstError
import com.booleworks.kjobs.data.Heartbeat
import com.booleworks.kjobs.data.Job
import com.booleworks.kjobs.data.JobStatus
import com.booleworks.kjobs.data.PersistenceAccessResult
import com.booleworks.kjobs.data.internalError
import com.booleworks.kjobs.data.notFound
import com.booleworks.kjobs.data.result
import com.booleworks.kjobs.data.success
import com.booleworks.kjobs.data.uuidNotFound
import org.slf4j.LoggerFactory
import redis.clients.jedis.JedisPool
import redis.clients.jedis.Pipeline
import redis.clients.jedis.Response
import redis.clients.jedis.Transaction
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

private val logger = LoggerFactory.getLogger(RedisDataPersistence::class.java)

/**
 * [JobPersistence] implementation for Redis. It requires a [JedisPool] providing the connection to
 * the Redis instance and a [Redis configuration][config].
 */
open class RedisJobPersistence(
    protected val pool: JedisPool,
    protected val config: RedisConfig = DefaultRedisConfig(),
) : JobPersistence {
    override suspend fun transaction(block: suspend JobTransactionalPersistence.() -> Unit): PersistenceAccessResult<Unit> = pool.resource.use { jedis ->
        jedis.multi().run {
            runCatching {
                RedisJobTransactionalPersistence(this@run, config)
                    .run { block() }
                    .also { exec() }
            }.onFailure { exception -> return handleTransactionException(exception) }
        }
        PersistenceAccessResult.success
    }

    override suspend fun fetchAllJobs(): PersistenceAccessResult<List<Job>> = getAllJobsBy<Unit>()

    override suspend fun fetchJob(uuid: String): PersistenceAccessResult<Job> =
        pool.resource.use { it.hgetAll(config.jobKey(uuid)) }
            .ifEmpty { return@fetchJob PersistenceAccessResult.uuidNotFound(uuid) }
            .redisMapToJob(uuid)

    override suspend fun fetchHeartbeats(since: LocalDateTime): PersistenceAccessResult<List<Heartbeat>> {
        val heartbeatKeys = pool.resource.use { it.keys(config.heartbeatPattern) }.toTypedArray().ifEmpty { return PersistenceAccessResult.result(emptyList()) }
        val plainHeartbeats = pool.resource.use { it.mget(*heartbeatKeys) } ?: run { return PersistenceAccessResult.notFound() }
        val filteredHeartbeats = plainHeartbeats.mapIndexed { index, beat ->
            Heartbeat(config.extractInstanceName(heartbeatKeys[index]), LocalDateTime.parse(beat))
        }.filter { !it.lastBeat.isBefore(since) }
        return PersistenceAccessResult.result(filteredHeartbeats)
    }

    override suspend fun allJobsWithStatus(status: JobStatus): PersistenceAccessResult<List<Job>> =
        getAllJobsBy({ transaction, key -> transaction.hget(key, "status") }) { it == status.toString() }

    override suspend fun allJobsOfInstance(status: JobStatus, instance: String): PersistenceAccessResult<List<Job>> =
        getAllJobsBy({ transaction, key -> transaction.hmget(key, "status", "executingInstance") }) { it == listOf(status.toString(), instance) }

    override suspend fun allJobsFinishedBefore(date: LocalDateTime): PersistenceAccessResult<List<Job>> =
        getAllJobsBy({ transaction, key -> transaction.hmget(key, "status", "finishedAt") }) { statusAndFinishedAt ->
            (statusAndFinishedAt[0] == JobStatus.SUCCESS.toString() || statusAndFinishedAt[0] == JobStatus.FAILURE.toString())
                    && statusAndFinishedAt.getOrNull(1)?.let { LocalDateTime.parse(it) }?.isBefore(date) ?: false
        }

    override suspend fun fetchStates(uuids: List<String>): PersistenceAccessResult<List<JobStatus>> {
        // TODO use pipelining
        return PersistenceAccessResult.result(
            pool.resource.use { jedis ->
                uuids.map { uuid ->
                    jedis.hget(config.jobKey(uuid), "status")?.let { status -> JobStatus.valueOf(status) }
                        ?: return@fetchStates PersistenceAccessResult.uuidNotFound(uuid)
                }
            })
    }

    private fun <T> getAllJobsBy(query: ((Pipeline, String) -> Response<T>)? = null, condition: ((T) -> Boolean)? = null): PersistenceAccessResult<List<Job>> {
        return pool.resource.use { jedis ->
            val allJobKeys = jedis.keys(config.jobPattern)
            val relevantKeys = if (query != null && condition != null) {
                val keyQueries = jedis.pipelined().use { pipeline ->
                    allJobKeys.map { it to query(pipeline, it) }.also { pipeline.sync() }
                }
                keyQueries.filter { condition(it.second.get()) }.map { it.first }
            } else {
                allJobKeys
            }
            jedis.pipelined().use { pipeline ->
                relevantKeys.map { it to pipeline.hgetAll(it) }.also { pipeline.sync() }
            }.map { it.second.get().redisMapToJob(config.extractUuid(it.first)) }
        }.unwrapOrReturnFirstError { return@getAllJobsBy it }
    }
}

/**
 * [JobTransactionalPersistence] implementation for Redis.
 *
 * Note that all methods will always return `PersistenceAccessResult.success`, since commands within transaction are not yet
 * executed, so the only real kind of error would be connection problems which are ok to be caught in [RedisJobPersistence.transaction].
 */
open class RedisJobTransactionalPersistence(
    protected val transaction: Transaction,
    protected val config: RedisConfig
) : JobTransactionalPersistence {
    override suspend fun persistJob(job: Job): PersistenceAccessResult<Unit> {
        transaction.hset(config.jobKey(job.uuid), job.toRedisMap())
        return PersistenceAccessResult.success
    }

    override suspend fun updateJob(job: Job): PersistenceAccessResult<Unit> {
        persistJob(job)
        job.nullFields().takeIf { it.isNotEmpty() }?.let { transaction.hdel(config.jobKey(job.uuid), *it.toTypedArray()) }
        return PersistenceAccessResult.success
    }

    override suspend fun deleteForUuid(uuid: String, persistencesPerType: Map<String, DataPersistence<*, *>>): PersistenceAccessResult<Unit> {
        transaction.del(config.jobKey(uuid))
        transaction.del(config.inputKey(uuid))
        transaction.del(config.resultKey(uuid))
        transaction.del(config.failureKey(uuid))
        return PersistenceAccessResult.success
    }

    override suspend fun updateHeartbeat(heartbeat: Heartbeat): PersistenceAccessResult<Unit> {
        transaction.set(config.heartbeatKey(heartbeat.instanceName), heartbeat.lastBeat.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
        return PersistenceAccessResult.success
    }
}

/**
 * [DataPersistence] implementation for Redis. It requires a [JedisPool] providing the connection to
 * the Redis instance, serializers and deserializers for job inputs and results, and a
 * [Redis configuration][config].
 */
open class RedisDataPersistence<INPUT, RESULT>(
    pool: JedisPool,
    protected val inputSerializer: (INPUT) -> ByteArray,
    protected val resultSerializer: (RESULT) -> ByteArray,
    protected val inputDeserializer: (ByteArray) -> INPUT,
    protected val resultDeserializer: (ByteArray) -> RESULT,
    config: RedisConfig = DefaultRedisConfig(),
) : RedisJobPersistence(pool, config), DataPersistence<INPUT, RESULT> {
    override suspend fun transaction(block: suspend JobTransactionalPersistence.() -> Unit): PersistenceAccessResult<Unit> = dataTransaction(block)

    override suspend fun <T> dataTransaction(block: suspend DataTransactionalPersistence<INPUT, RESULT>.() -> T): PersistenceAccessResult<T> =
        pool.resource.use { jedis ->
            jedis.multi().run {
                runCatching {
                    RedisDataTransactionalPersistence(this@run, inputSerializer, resultSerializer, config)
                        .run { block() }
                        .also { exec() }
                }.getOrElse { exception -> return handleTransactionException(exception) }
            }
        }.let { Either.Right(it) }

    override suspend fun fetchInput(uuid: String): PersistenceAccessResult<INPUT> {
        val inputBytes = pool.resource.use { it.get(config.inputKey(uuid).toByteArray()) } ?: run { return PersistenceAccessResult.uuidNotFound(uuid) }
        return PersistenceAccessResult.result(inputDeserializer(inputBytes))
    }

    override suspend fun fetchResult(uuid: String): PersistenceAccessResult<RESULT> {
        val resultBytes = pool.resource.use { it.get(config.resultKey(uuid).toByteArray()) } ?: run { return PersistenceAccessResult.uuidNotFound(uuid) }
        return PersistenceAccessResult.result(resultDeserializer(resultBytes))
    }

    override suspend fun fetchFailure(uuid: String): PersistenceAccessResult<String> {
        return pool.resource.use { it.get(config.failureKey(uuid)) }?.let { PersistenceAccessResult.result(it) }
            ?: run { return PersistenceAccessResult.uuidNotFound(uuid) }
    }
}

/**
 * [DataTransactionalPersistence] implementation for Redis.
 *
 * Note that all methods will always return `PersistenceAccessResult.success`, since commands within transaction are not yet
 * executed, so the only real kind of error would be connection problems which are ok to be caught in [RedisDataPersistence.dataTransaction].
 */
open class RedisDataTransactionalPersistence<INPUT, RESULT>(
    transaction: Transaction,
    protected val inputSerializer: (INPUT) -> ByteArray,
    protected val resultSerializer: (RESULT) -> ByteArray,
    config: RedisConfig
) : RedisJobTransactionalPersistence(transaction, config), DataTransactionalPersistence<INPUT, RESULT> {

    override suspend fun persistInput(job: Job, input: INPUT): PersistenceAccessResult<Unit> {
        transaction.set(config.inputKey(job.uuid).toByteArray(), inputSerializer(input))
        return PersistenceAccessResult.success
    }

    override suspend fun persistOrUpdateResult(job: Job, result: RESULT): PersistenceAccessResult<Unit> {
        transaction.set(config.resultKey(job.uuid).toByteArray(), resultSerializer(result))
        return PersistenceAccessResult.success
    }

    override suspend fun persistOrUpdateFailure(job: Job, failure: String): PersistenceAccessResult<Unit> {
        transaction.set(config.failureKey(job.uuid), failure)
        return PersistenceAccessResult.success
    }
}

private fun <T> Transaction.handleTransactionException(ex: Throwable): PersistenceAccessResult<T> {
    val message = ex.message ?: "Undefined error"
    logger.error("Jedis transaction failed with: $message", ex)
    val discardResult = runCatching { discard() }.onFailure { discardException -> logger.error("Discarding transaction failed", discardException) }
    logger.error("Discarded the transaction with result: $discardResult")
    return PersistenceAccessResult.internalError(message)
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
