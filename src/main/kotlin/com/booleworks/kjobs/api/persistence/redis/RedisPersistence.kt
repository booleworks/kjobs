// SPDX-License-Identifier: MIT
// Copyright 2023-2024 BooleWorks GmbH

package com.booleworks.kjobs.api.persistence.redis

import com.booleworks.kjobs.api.persistence.DataPersistence
import com.booleworks.kjobs.api.persistence.DataTransactionalPersistence
import com.booleworks.kjobs.api.persistence.JobPersistence
import com.booleworks.kjobs.api.persistence.JobTransactionalPersistence
import com.booleworks.kjobs.common.unwrapOrReturnFirstError
import com.booleworks.kjobs.control.logTime
import com.booleworks.kjobs.data.Heartbeat
import com.booleworks.kjobs.data.Job
import com.booleworks.kjobs.data.JobStatus
import com.booleworks.kjobs.data.PersistenceAccessResult
import com.booleworks.kjobs.data.internalError
import com.booleworks.kjobs.data.modified
import com.booleworks.kjobs.data.notFound
import com.booleworks.kjobs.data.orQuitWith
import com.booleworks.kjobs.data.result
import com.booleworks.kjobs.data.success
import com.booleworks.kjobs.data.uuidNotFound
import io.lettuce.core.KeyValue
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisFuture
import io.lettuce.core.ScanArgs
import io.lettuce.core.ScanIterator
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.SetArgs
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.codec.CompressionCodec
import io.lettuce.core.codec.CompressionCodec.CompressionType.GZIP
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import org.intellij.lang.annotations.Language
import org.slf4j.LoggerFactory
import org.slf4j.event.Level
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import kotlin.time.toJavaDuration


private val logger = LoggerFactory.getLogger("com.booleworks.kjobs.RedisDataPersistence")

/**
 * [JobPersistence] implementation for Redis based on [Lettuce](https://lettuce.io).
 * It requires a [RedisClient] providing the connection to the Redis instance and
 * a [Redis configuration][config].
 */
open class RedisJobPersistence(
    val redisClient: RedisClient,
    protected val config: RedisConfig = DefaultRedisConfig(),
) : JobPersistence {

    // the standard connection can be shared among threads without locking since it is NOT used for pipelines (setAutoFlushCommands = false/true)
    // NOR for transactions (multi/exec)
    protected val standardConnection: StatefulRedisConnection<String, String> = redisClient.connect()
    protected val standardStringCommands: RedisAsyncCommands<String, String> = standardConnection.async()

    protected val pipelineConnectionMutex = Mutex()
    protected val pipelineConnection: StatefulRedisConnection<String, String> = redisClient.connect()

    protected val transactionStringConnectionMutex = Mutex()
    protected val transactionStringConnection: StatefulRedisConnection<String, String> = redisClient.connect()

    override suspend fun transaction(block: suspend JobTransactionalPersistence.() -> Unit): PersistenceAccessResult<Unit> = try {
        transactionStringConnectionMutex.withLock {
            val commands = transactionStringConnection.async()
            commands.multi().get()
            RedisJobTransactionalPersistence(commands, config).run { block() }
            commands.exec().get()
        }
        PersistenceAccessResult.success
    } catch (exception: Exception) {
        handleTransactionException(exception)
    }

    override suspend fun transactionWithPreconditions(
        preconditions: Map<String, (Job) -> Boolean>,
        block: suspend JobTransactionalPersistence.() -> Unit
    ): PersistenceAccessResult<Unit> = try {
        val transactionResult = transactionStringConnectionMutex.withLock {
            val commands = transactionStringConnection.async()
            preconditions.forEach { (uuid, condition) ->
                commands.watch(uuid)
                val job = commands.hgetall(config.jobKey(uuid)).get()
                    .ifEmpty { return PersistenceAccessResult.uuidNotFound(uuid) }
                    .redisMapToJob(uuid).rightOr { return PersistenceAccessResult.internalError("Failed to convert internal job") }
                if (!condition(job)) {
                    logger.debug("Precondition for updating job with UUID $uuid failed.")
                    return PersistenceAccessResult.modified()
                }
            }
            logTime(logger, Level.TRACE, { "Transaction with precondition in $it" }) {
                commands.multi().get()
                RedisJobTransactionalPersistence(commands, config).run { block() }
                commands.exec().get()
            }
        }
        if (transactionResult == null || transactionResult.wasDiscarded()) {
            logger.debug(
                "Transaction with precondition was not executed ({})",
                transactionResult?.let { "Transaction was discarded" } ?: "Transaction was null")
            PersistenceAccessResult.modified()
        } else {
            PersistenceAccessResult.success
        }
    } catch (exception: Exception) {
        handleTransactionException(exception)
    }

    override suspend fun tryReserveJob(job: Job, instanceName: String): PersistenceAccessResult<Unit> {
        try {
            val keyArguments: Array<String> = arrayOf(config.jobKey(job.uuid))
            val valueArguments: Array<String?> = arrayOf(instanceName, job.startedAt?.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME), job.timeout?.toString())
            return when (val result =
                standardStringCommands.eval<String>(LuaScripts.reserveJobIfStatusIsCreated, ScriptOutputType.STATUS, keyArguments, *valueArguments).get()) {
                "OK" -> PersistenceAccessResult.success
                "MODIFIED" -> PersistenceAccessResult.modified()
                else -> PersistenceAccessResult.internalError("Unknown result: $result")
            }
        } catch (e: Exception) {
            logger.error("Trying to reserve job failed with exception.", e)
            return PersistenceAccessResult.internalError(e.message ?: "Unknown error")
        }
    }

    override suspend fun updateJobTimeout(uuid: String, timeout: LocalDateTime?): PersistenceAccessResult<Unit> {
        standardStringCommands.hset(config.jobKey(uuid), "timeout", timeout?.toString())
        return PersistenceAccessResult.success
    }

    override suspend fun fetchAllJobs(): PersistenceAccessResult<List<Job>> =
        logTime(logger, Level.TRACE, { "Fetched all jobs in $it" }) { getAllJobsBy<Unit>() }

    override suspend fun fetchJob(uuid: String): PersistenceAccessResult<Job> = withContext(Dispatchers.IO) {
        standardStringCommands.hgetall(config.jobKey(uuid)).get()
            .ifEmpty { return@withContext PersistenceAccessResult.uuidNotFound(uuid) }
            .redisMapToJob(uuid)
    }

    override suspend fun updateHeartbeat(heartbeat: Heartbeat): PersistenceAccessResult<Unit> {
        // heartbeat with expiration to prevent heartbeat entries of previous instances to remain forever in Redis
        val setArgs = SetArgs.Builder.ex(config.heartbeatExpiration.toJavaDuration())
        standardStringCommands.set(config.heartbeatKey(heartbeat.instanceName), heartbeat.lastBeat.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME), setArgs)
        return PersistenceAccessResult.success
    }

    override suspend fun fetchHeartbeat(instanceName: String, since: LocalDateTime): PersistenceAccessResult<Heartbeat?> = withContext(Dispatchers.IO) {
        val heartbeatKey = config.heartbeatKey(instanceName)
        val lastBeat = standardStringCommands.get(heartbeatKey).get() ?: run { return@withContext PersistenceAccessResult.notFound() }
        val heartbeat = Heartbeat(instanceName, LocalDateTime.parse(lastBeat))
        return@withContext PersistenceAccessResult.result(if (heartbeat.lastBeat.isBefore(since)) null else heartbeat)
    }

    override suspend fun fetchHeartbeats(since: LocalDateTime): PersistenceAccessResult<List<Heartbeat>> = withContext(Dispatchers.IO) {
        val heartbeatKeys = standardConnection.scanKeys(config.heartbeatPattern).toTypedArray()
            .ifEmpty { return@withContext PersistenceAccessResult.result(emptyList()) }
        val plainHeartbeats = standardStringCommands.mget(*heartbeatKeys).get() ?: run { return@withContext PersistenceAccessResult.notFound() }
        val filteredHeartbeats = plainHeartbeats
            .map { beat -> Heartbeat(config.extractInstanceName(beat.key), LocalDateTime.parse(beat.value)) }
            .filter { !it.lastBeat.isBefore(since) }
        PersistenceAccessResult.result(filteredHeartbeats)
    }

    override suspend fun allJobsWithStatus(status: JobStatus): PersistenceAccessResult<List<Job>> =
        logTime(logger, Level.TRACE, { "Fetching all jobs with $status in $it" }) {
            getAllJobsBy(status, config.jobPattern)
        }

    override suspend fun allJobsOfInstance(status: JobStatus, instance: String): PersistenceAccessResult<List<Job>> =
        logTime(logger, Level.TRACE, { "Fetching all jobs of $status and instance $instance in $it" }) {
            getAllJobsBy({ commands, key -> commands.hmget(key, "status", "executingInstance") }) {
                it.get("status") == status.toString() && it.get("executingInstance") == instance
            }
        }

    override suspend fun allJobsFinishedBefore(date: LocalDateTime): PersistenceAccessResult<List<Job>> =
        logTime(logger, Level.TRACE, { "Fetching all jobs finished before $date in $it" }) {
            getAllJobsBy({ commands, key -> commands.hmget(key, "status", "finishedAt") }) {
                it.get("status") in setOf(JobStatus.SUCCESS.toString(), JobStatus.FAILURE.toString(), JobStatus.CANCELLED.toString())
                        && it.get("finishedAt")?.let(LocalDateTime::parse)?.isBefore(date) ?: false
            }
        }

    override suspend fun allJobsExceedingDbJobCount(maxNumberKeptJobs: Int): PersistenceAccessResult<List<Job>> {
        val overallKeyCount = standardConnection.scanKeys(config.jobPattern).count()
        val exceedingJobCount = overallKeyCount - maxNumberKeptJobs
        return if (exceedingJobCount > 0) {
            logTime(logger, Level.TRACE, { "Fetched jobs for job count in $it" }) {
                getAllJobsBy({ commands, key -> commands.hmget(key, "status") }) {
                    it.get("status") in setOf(JobStatus.SUCCESS.toString(), JobStatus.FAILURE.toString(), JobStatus.CANCELLED.toString())
                }.orQuitWith {
                    logger.error("Failed fetching jobs for job count: " + it.message)
                    return PersistenceAccessResult.internalError<List<Job>>(it.message)
                }
            }.let { jobs ->
                PersistenceAccessResult.result(jobs.sortedBy { it.createdAt }.take(exceedingJobCount))
            }
        } else {
            PersistenceAccessResult.result(emptyList())
        }
    }

    override suspend fun fetchStates(uuids: List<String>): PersistenceAccessResult<List<JobStatus>> {
        // TODO use pipelining
        return PersistenceAccessResult.result(
            uuids.map { uuid ->
                standardStringCommands.hget(config.jobKey(uuid), "status").get()?.let { status -> JobStatus.valueOf(status) }
                    ?: return@fetchStates PersistenceAccessResult.uuidNotFound(uuid)
            })
    }

    private suspend fun <T> getAllJobsBy(
        query: ((RedisAsyncCommands<String, String>, String) -> RedisFuture<T>)? = null,
        condition: ((T) -> Boolean)? = null
    ): PersistenceAccessResult<List<Job>> = pipelineConnectionMutex.withLock {
        val stringCommands = pipelineConnection.async()
        val allJobKeys = pipelineConnection.scanKeys(config.jobPattern)
        pipelineConnection.setAutoFlushCommands(false)
        val relevantKeys = if (query != null && condition != null) {
            val keyQueries = allJobKeys.map { it to query(stringCommands, it) }
            pipelineConnection.flushCommands()
            keyQueries.filter { condition(it.second.get()) }.map { it.first }
        } else {
            allJobKeys
        }
        val jobFutures = relevantKeys.map { it to stringCommands.hgetall(it) }
        pipelineConnection.flushCommands()
        val result = jobFutures.map { it.second.get().redisMapToJob(config.extractUuid(it.first)) }
            .unwrapOrReturnFirstError { return@getAllJobsBy it }

        pipelineConnection.setAutoFlushCommands(true)
        return result
    }

    private fun StatefulRedisConnection<String, String>.scanKeys(keyPattern: String): List<String> =
        logTime(logger, Level.TRACE, { "Scanned keys of pattern $keyPattern in $it" }) {
            ScanIterator.scan(sync(), ScanArgs().match(keyPattern).limit(config.scanLimit)).asSequence().toList()
        }

    private fun getAllJobsBy(status: JobStatus, jobPattern: String): PersistenceAccessResult<List<Job>> {
        val valueArguments: Array<String> = arrayOf(status.name, jobPattern, config.scanLimit.toString())
        val jobs = standardStringCommands.eval<List<List<String>>>(LuaScripts.filterJobs, ScriptOutputType.OBJECT, arrayOf<String>(), *valueArguments).get()
        return jobs.map {
            val redisMap = it.redisListToRedisMap()
            redisMap.redisMapToJob(config.extractUuid(redisMap["redis-key"]!!))
        }.unwrapOrReturnFirstError { return@getAllJobsBy it }
    }

    /**
     * Converts a Redis list with map data into a map instance.
     * Assumption: The Redis list contains the map entries as consecutive key-value list entries.
     * Example: `["key1", "value1", "key2", "value2"]`
     */
    private fun List<String>.redisListToRedisMap(): Map<String, String> {
        if (size % 2 == 1) {
            error("Unexpected number of elements in Redis Job list")
        }
        return 0.until(size / 2).associate { i ->
            val key = get(i * 2)
            val value = get(i * 2 + 1)
            key to value
        }
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

    // the standard connection can be shared among threads without looking since it is NOT used for pipelines (setAutoFlushCommands = false/true)
    // NOR for transactions (multi/exec)
    protected val standardByteArrayConnection: StatefulRedisConnection<ByteArray, ByteArray> = newByteArrayConnection()
    protected val standardByteArrayCommands: RedisAsyncCommands<ByteArray, ByteArray> = standardByteArrayConnection.async()

    protected val transactionByteArrayConnectionMutex = Mutex()
    protected val transactionByteArrayConnection: StatefulRedisConnection<ByteArray, ByteArray> = newByteArrayConnection()

    protected fun newByteArrayConnection(): StatefulRedisConnection<ByteArray, ByteArray> = redisClient
        .connect(if (config.useCompression) CompressionCodec.valueCompressor(ByteArrayCodec.INSTANCE, GZIP) else ByteArrayCodec.INSTANCE)

    override suspend fun transaction(block: suspend JobTransactionalPersistence.() -> Unit): PersistenceAccessResult<Unit> = dataTransaction(block)

    override suspend fun <T> dataTransaction(block: suspend DataTransactionalPersistence<INPUT, RESULT>.() -> T): PersistenceAccessResult<T> =
        transactionStringConnectionMutex.withLock {
            transactionByteArrayConnectionMutex.withLock {
                val stringCommandsForTransaction = transactionStringConnection.async()
                val byteArrayCommandsForTransaction = transactionByteArrayConnection.async()
                return try {
                    stringCommandsForTransaction.multi().get()
                    byteArrayCommandsForTransaction.multi().get()
                    RedisDataTransactionalPersistence(stringCommandsForTransaction, byteArrayCommandsForTransaction, inputSerializer, resultSerializer, config)
                        .run { block() }
                        .also {
                            // The order here is relevant for storing new jobs. If we finish the string commands transaction first,
                            // other instances/threads may find the new job but not its input (especially since inputs may be quite large).
                            byteArrayCommandsForTransaction.exec().get()
                            stringCommandsForTransaction.exec().get()
                        }.let { PersistenceAccessResult.result(it) }
                } catch (exception: Exception) {
                    // TODO this may fail if one transaction already succeeded
                    stringCommandsForTransaction.discard().get()
                    byteArrayCommandsForTransaction.discard().get()
                    handleTransactionException(exception)
                }
            }
        }

    override suspend fun fetchInput(uuid: String): PersistenceAccessResult<INPUT> = withContext(Dispatchers.IO) {
        standardByteArrayCommands.get(config.inputKey(uuid).toByteArray()).get()
            ?.let { PersistenceAccessResult.result(inputDeserializer(it)) }
            ?: PersistenceAccessResult.uuidNotFound(uuid)
    }

    override suspend fun fetchResult(uuid: String): PersistenceAccessResult<RESULT> = withContext(Dispatchers.IO) {
        standardByteArrayCommands.get(config.resultKey(uuid).toByteArray()).get()
            ?.let { PersistenceAccessResult.result(resultDeserializer(it)) }
            ?: PersistenceAccessResult.uuidNotFound(uuid)
    }

    override suspend fun fetchFailure(uuid: String): PersistenceAccessResult<String> = withContext(Dispatchers.IO) {
        standardStringCommands.get(config.failureKey(uuid)).get()
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
    logger.error("Redis transaction failed with: $message", ex)
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

fun <K, V> List<KeyValue<K, V>>.get(key: K): V? = firstOrNull { it.key == key }?.getValueOrElse(null)

private object LuaScripts {

    // input: 1 key (job key), 3 values (executing instance, started at, timeout)
    // output: "OK" if reservation was successful, "MODIFIED" if job was already reserved
    @Language("Lua")
    val reserveJobIfStatusIsCreated = """
        local jobId = KEYS[1]
        local instanceName = ARGV[1]
        local startedAt = ARGV[2]
        local timeout = ARGV[3]

        if redis.call("hget", jobId,"status") == "CREATED"
        then
            return redis.call("hmset", jobId, "executingInstance", instanceName, "startedAt", startedAt, "status", "RUNNING", "timeout", timeout)
        else
            return "MODIFIED"
        end
    """.trimIndent()

    // input: 0 keys, 3 values (status, job pattern, scan limit)
    // output: list of all jobs that match the given status
    @Language("Lua")
    val filterJobs = """
        local status = ARGV[1]
        local jobPattern = ARGV[2]
        local scanLimit = ARGV[3]

        local cursor = "0"
        local jobs = {}

        repeat
            local result = redis.call("SCAN", cursor, "MATCH", jobPattern, "COUNT", scanLimit)
            cursor = result[1]
            local keys = result[2]

            for _, key in ipairs(keys) do
                local jobStatus = redis.call("HGET", key, "status")
                if jobStatus == status then
                    local all = redis.call("HGETALL", key)
                    -- we add an additional pair ("redis-key" to key) to create the Job instance more easily later
                    table.insert(all, "redis-key")
                    table.insert(all, key)
                    table.insert(jobs, all)
                end
            end
        until cursor == "0"
        return jobs
    """.trimIndent()
}
