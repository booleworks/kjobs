// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.api.persistence.redis

import com.booleworks.kjobs.api.persistence.DataPersistence
import com.booleworks.kjobs.api.persistence.DataTransactionalPersistence
import com.booleworks.kjobs.api.persistence.JobPersistence
import com.booleworks.kjobs.api.persistence.JobTransactionalPersistence
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
import io.lettuce.core.KeyValue
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisFuture
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.codec.CompressionCodec
import io.lettuce.core.codec.CompressionCodec.CompressionType.GZIP
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

private val logger = LoggerFactory.getLogger("RedisDataPersistence")

/**
 * [JobPersistence] implementation for Redis based on [Lettuce](https://lettuce.io).
 * It requires a [RedisClient] providing the connection to the Redis instance and
 * a [Redis configuration][config].
 */
open class RedisJobPersistence(
    val redisClient: RedisClient,
    protected val config: RedisConfig = DefaultRedisConfig(),
) : JobPersistence {
    protected val stringCommands = newStringCommands()
    protected fun newStringCommands() = redisClient.connect().async()!!

    override suspend fun transaction(block: suspend JobTransactionalPersistence.() -> Unit): PersistenceAccessResult<Unit> {
        runCatching {
            val commands = newStringCommands()
            commands.multi().get()
            RedisJobTransactionalPersistence(commands, config).run { block() }
            commands.exec().get()
        }.onFailure { exception -> return handleTransactionException(exception) }
        return PersistenceAccessResult.success
    }

    override suspend fun fetchAllJobs(): PersistenceAccessResult<List<Job>> = getAllJobsBy<Unit>()

    override suspend fun fetchJob(uuid: String): PersistenceAccessResult<Job> = withContext(Dispatchers.IO) {
        stringCommands.hgetall(config.jobKey(uuid)).get()
            .ifEmpty { return@withContext PersistenceAccessResult.uuidNotFound(uuid) }
            .redisMapToJob(uuid)
    }

    override suspend fun fetchHeartbeats(since: LocalDateTime): PersistenceAccessResult<List<Heartbeat>> = withContext(Dispatchers.IO) {
        val heartbeatKeys = stringCommands.keys(config.heartbeatPattern).get().toTypedArray()
            .ifEmpty { return@withContext PersistenceAccessResult.result(emptyList()) }
        val plainHeartbeats = stringCommands.mget(*heartbeatKeys).get() ?: run { return@withContext PersistenceAccessResult.notFound() }
        val filteredHeartbeats = plainHeartbeats
            .map { beat -> Heartbeat(config.extractInstanceName(beat.key), LocalDateTime.parse(beat.value)) }
            .filter { !it.lastBeat.isBefore(since) }
        PersistenceAccessResult.result(filteredHeartbeats)
    }

    override suspend fun allJobsWithStatus(status: JobStatus): PersistenceAccessResult<List<Job>> =
        getAllJobsBy({ transaction, key -> transaction.hget(key, "status") }) { it == status.toString() }

    override suspend fun allJobsOfInstance(status: JobStatus, instance: String): PersistenceAccessResult<List<Job>> =
        getAllJobsBy({ commands, key -> commands.hmget(key, "status", "executingInstance") }) {
            it.get("status") == status.toString() && it.get("executingInstance") == instance
        }

    override suspend fun allJobsFinishedBefore(date: LocalDateTime): PersistenceAccessResult<List<Job>> =
        getAllJobsBy({ commands, key -> commands.hmget(key, "status", "finishedAt") }) {
            it.get("status") in setOf(JobStatus.SUCCESS.toString(), JobStatus.FAILURE.toString())
                    && it.get("finishedAt")?.let(LocalDateTime::parse)?.isBefore(date) ?: false
        }

    override suspend fun fetchStates(uuids: List<String>): PersistenceAccessResult<List<JobStatus>> {
        // TODO use pipelining
        return PersistenceAccessResult.result(
            uuids.map { uuid ->
                stringCommands.hget(config.jobKey(uuid), "status").get()?.let { status -> JobStatus.valueOf(status) }
                    ?: return@fetchStates PersistenceAccessResult.uuidNotFound(uuid)
            })
    }

    private fun <T> getAllJobsBy(
        query: ((RedisAsyncCommands<String, String>, String) -> RedisFuture<T>)? = null,
        condition: ((T) -> Boolean)? = null
    ): PersistenceAccessResult<List<Job>> = redisClient.connect().use { redisConnection ->
        val stringCommands = redisConnection.async()
        val allJobKeys = stringCommands.keys(config.jobPattern).get()
        redisConnection.setAutoFlushCommands(false)
        val relevantKeys = if (query != null && condition != null) {
            val keyQueries = allJobKeys.map { it to query(stringCommands, it) }
            redisConnection.flushCommands()
            keyQueries.filter { condition(it.second.get()) }.map { it.first }
        } else {
            allJobKeys
        }
        val jobFutures = relevantKeys.map { it to stringCommands.hgetall(it) }
        redisConnection.flushCommands()
        jobFutures.map { it.second.get().redisMapToJob(config.extractUuid(it.first)) }
            .unwrapOrReturnFirstError { return@getAllJobsBy it }
    }
}

/**
 * [JobTransactionalPersistence] implementation for Redis.
 *
 * Note that all methods will always return `PersistenceAccessResult.success`, since commands within transaction are not yet
 * executed, so the only real kind of error would be connection problems which are ok to be caught in [RedisJobPersistence.transaction].
 */
open class RedisJobTransactionalPersistence(
    protected val stringCommands: RedisAsyncCommands<String, String>,
    protected val config: RedisConfig
) : JobTransactionalPersistence {
    override suspend fun persistJob(job: Job): PersistenceAccessResult<Unit> {
        stringCommands.hset(config.jobKey(job.uuid), job.toRedisMap())
        return PersistenceAccessResult.success
    }

    override suspend fun updateJob(job: Job): PersistenceAccessResult<Unit> {
        persistJob(job)
        job.nullFields().takeIf { it.isNotEmpty() }?.let { stringCommands.hdel(config.jobKey(job.uuid), *it.toTypedArray()) }
        return PersistenceAccessResult.success
    }

    override suspend fun deleteForUuid(uuid: String, persistencesPerType: Map<String, DataPersistence<*, *>>): PersistenceAccessResult<Unit> {
        stringCommands.del(config.jobKey(uuid), config.inputKey(uuid), config.resultKey(uuid), config.failureKey(uuid))
        return PersistenceAccessResult.success
    }

    override suspend fun updateHeartbeat(heartbeat: Heartbeat): PersistenceAccessResult<Unit> {
        stringCommands.set(config.heartbeatKey(heartbeat.instanceName), heartbeat.lastBeat.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
        return PersistenceAccessResult.success
    }
}

/**
 * [DataPersistence] implementation for Redis based on [Lettuce](https://lettuce.io).
 * It requires a [RedisClient] providing the connection to the Redis instance, serializers
 * and deserializers for job inputs and results, and a [Redis configuration][config].
 */
open class RedisDataPersistence<INPUT, RESULT>(
    redisClient: RedisClient,
    protected val inputSerializer: (INPUT) -> ByteArray,
    protected val resultSerializer: (RESULT) -> ByteArray,
    protected val inputDeserializer: (ByteArray) -> INPUT,
    protected val resultDeserializer: (ByteArray) -> RESULT,
    config: RedisConfig = DefaultRedisConfig(),
) : RedisJobPersistence(redisClient, config), DataPersistence<INPUT, RESULT> {
    protected val byteArrayCommands: RedisAsyncCommands<ByteArray, ByteArray> = newByteArrayCommands()
    protected fun newByteArrayCommands(): RedisAsyncCommands<ByteArray, ByteArray> = redisClient
        .connect(if (config.useCompression) CompressionCodec.valueCompressor(ByteArrayCodec.INSTANCE, GZIP) else ByteArrayCodec.INSTANCE)
        .async()

    override suspend fun transaction(block: suspend JobTransactionalPersistence.() -> Unit): PersistenceAccessResult<Unit> = dataTransaction(block)

    override suspend fun <T> dataTransaction(block: suspend DataTransactionalPersistence<INPUT, RESULT>.() -> T): PersistenceAccessResult<T> {
        val stringCommandsForTransaction = newStringCommands()
        val byteArrayCommandsForTransaction = newByteArrayCommands()
        return runCatching {
            stringCommandsForTransaction.multi().get()
            byteArrayCommandsForTransaction.multi().get()
            RedisDataTransactionalPersistence(stringCommandsForTransaction, byteArrayCommandsForTransaction, inputSerializer, resultSerializer, config)
                .run { block() }
                .also {
                    // The order here is relevant for storing new jobs. If we finish the string commands transaction first,
                    // other instances/threads may find the new job but not its input (especially since inputs may be quite large).
                    byteArrayCommandsForTransaction.exec().get()
                    stringCommandsForTransaction.exec().get()
                }
        }.getOrElse { exception ->
            // TODO this may fail if one transaction already succeeded
            stringCommandsForTransaction.discard().get()
            byteArrayCommandsForTransaction.discard().get()
            return handleTransactionException(exception)
        }.let { PersistenceAccessResult.result(it) }
    }

    override suspend fun fetchInput(uuid: String): PersistenceAccessResult<INPUT> = withContext(Dispatchers.IO) {
        byteArrayCommands.get(config.inputKey(uuid).toByteArray()).get()
            ?.let { PersistenceAccessResult.result(inputDeserializer(it)) }
            ?: PersistenceAccessResult.uuidNotFound(uuid)
    }

    override suspend fun fetchResult(uuid: String): PersistenceAccessResult<RESULT> = withContext(Dispatchers.IO) {
        byteArrayCommands.get(config.resultKey(uuid).toByteArray()).get()
            ?.let { PersistenceAccessResult.result(resultDeserializer(it)) }
            ?: PersistenceAccessResult.uuidNotFound(uuid)
    }

    override suspend fun fetchFailure(uuid: String): PersistenceAccessResult<String> = withContext(Dispatchers.IO) {
        stringCommands.get(config.failureKey(uuid)).get()
            ?.let { PersistenceAccessResult.result(it) }
            ?: PersistenceAccessResult.uuidNotFound(uuid)
    }
}

/**
 * [DataTransactionalPersistence] implementation for Redis.
 *
 * Note that all methods will always return `PersistenceAccessResult.success`, since commands within transaction are not yet
 * executed, so the only real kind of error would be connection problems which are ok to be caught in [RedisDataPersistence.dataTransaction].
 */
open class RedisDataTransactionalPersistence<INPUT, RESULT>(
    stringCommands: RedisAsyncCommands<String, String>,
    protected val byteArrayCommands: RedisAsyncCommands<ByteArray, ByteArray>,
    protected val inputSerializer: (INPUT) -> ByteArray,
    protected val resultSerializer: (RESULT) -> ByteArray,
    config: RedisConfig
) : RedisJobTransactionalPersistence(stringCommands, config), DataTransactionalPersistence<INPUT, RESULT> {

    override suspend fun persistInput(job: Job, input: INPUT): PersistenceAccessResult<Unit> {
        byteArrayCommands.set(config.inputKey(job.uuid).toByteArray(), inputSerializer(input))
        return PersistenceAccessResult.success
    }

    override suspend fun persistOrUpdateResult(job: Job, result: RESULT): PersistenceAccessResult<Unit> {
        byteArrayCommands.set(config.resultKey(job.uuid).toByteArray(), resultSerializer(result))
        return PersistenceAccessResult.success
    }

    override suspend fun persistOrUpdateFailure(job: Job, failure: String): PersistenceAccessResult<Unit> {
        stringCommands.set(config.failureKey(job.uuid), failure)
        return PersistenceAccessResult.success
    }
}

private fun <T> handleTransactionException(ex: Throwable): PersistenceAccessResult<T> {
    val message = ex.message ?: "Undefined error"
    logger.error("Jedis transaction failed with: $message", ex)
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

fun <K, V> List<KeyValue<K, V>>.get(key: K): V? = firstOrNull { it.key == key }?.value
