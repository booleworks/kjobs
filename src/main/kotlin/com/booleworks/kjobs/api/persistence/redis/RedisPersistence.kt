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
import glide.api.GlideClient
import glide.api.models.Batch
import glide.api.models.GlideString.gs
import glide.api.models.Script
import glide.api.models.commands.ScriptOptions
import glide.api.models.commands.SetOptions
import glide.api.models.commands.scan.ScanOptions
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import org.intellij.lang.annotations.Language
import org.slf4j.LoggerFactory
import org.slf4j.event.Level
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream


private val logger = LoggerFactory.getLogger("com.booleworks.kjobs.RedisDataPersistence")

/**
 * [JobPersistence] implementation for Redis based on [Lettuce](https://lettuce.io).
 * It requires a [GlideClient] providing the connection to the Redis instance and
 * a [Redis configuration][config].
 */
open class RedisJobPersistence(
    val redisClient: GlideClient,
    protected val config: RedisConfig = DefaultRedisConfig(),
) : JobPersistence {

    protected val redisClientMutex = Mutex()

    override suspend fun transaction(block: suspend JobTransactionalPersistence.() -> Unit): PersistenceAccessResult<Unit> = try {
        val transaction = Batch(true)
        RedisJobTransactionalPersistence(transaction, config).run { block() }
        redisClient.exec(transaction, true).get()
        PersistenceAccessResult.success
    } catch (exception: Exception) {
        handleTransactionException(exception)
    }

    override suspend fun transactionWithPreconditions(
        preconditions: Map<String, (Job) -> Boolean>,
        block: suspend JobTransactionalPersistence.() -> Unit
    ): PersistenceAccessResult<Unit> = try {
        preconditions.forEach { (uuid, condition) ->
            val job = redisClient.hgetall(config.jobKey(uuid)).get()
                .ifEmpty { return PersistenceAccessResult.uuidNotFound(uuid) }
                .redisMapToJob(uuid).rightOr { return PersistenceAccessResult.internalError("Failed to convert internal job") }
            if (!condition(job)) {
                logger.debug("Precondition for updating job with UUID $uuid failed.")
                return PersistenceAccessResult.modified()
            }
        }
        logTime(logger, Level.TRACE, { "Transaction with precondition in $it" }) {
            val transaction = Batch(true)
            RedisJobTransactionalPersistence(transaction, config).run { block() }
            redisClient.exec(transaction, true).get()
        }
        PersistenceAccessResult.success
    } catch (exception: Exception) {
        handleTransactionException(exception)
    }

    override suspend fun tryReserveJob(job: Job, instanceName: String): PersistenceAccessResult<Unit> {
        try {
            val keyArguments: List<String> = listOf(config.jobKey(job.uuid))
            val valueArguments: List<String?> = listOf(instanceName, job.startedAt?.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME), job.timeout?.toString())
            val scriptOptions = ScriptOptions.builder().keys(keyArguments).args(valueArguments).build()
            return when (val result = redisClient.invokeScript(Script(LuaScripts.reserveJobIfStatusIsCreated, false), scriptOptions).get()) {
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
        redisClient.hset(config.jobKey(uuid), mapOf("timeout" to timeout?.toString())).get()
        return PersistenceAccessResult.success
    }

    override suspend fun fetchAllJobs(): PersistenceAccessResult<List<Job>> =
        logTime(logger, Level.TRACE, { "Fetched all jobs in $it" }) { getAllJobsBy() }

    override suspend fun fetchJob(uuid: String): PersistenceAccessResult<Job> = withContext(Dispatchers.IO) {
        redisClient.hgetall(config.jobKey(uuid)).get()
            .ifEmpty { return@withContext PersistenceAccessResult.uuidNotFound(uuid) }
            .redisMapToJob(uuid)
    }

    override suspend fun updateHeartbeat(heartbeat: Heartbeat): PersistenceAccessResult<Unit> {
        // heartbeat with expiration to prevent heartbeat entries of previous instances to remain forever in Redis
        val setArgs = SetOptions.builder().expiry(SetOptions.Expiry.Seconds(config.heartbeatExpiration.inWholeSeconds)).build()
        redisClient.set(config.heartbeatKey(heartbeat.instanceName), heartbeat.lastBeat.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME), setArgs)
        return PersistenceAccessResult.success
    }

    override suspend fun fetchHeartbeat(instanceName: String, since: LocalDateTime): PersistenceAccessResult<Heartbeat?> = withContext(Dispatchers.IO) {
        val heartbeatKey = config.heartbeatKey(instanceName)
        val lastBeat = redisClient.get(heartbeatKey).get() ?: run { return@withContext PersistenceAccessResult.notFound() }
        val heartbeat = Heartbeat(instanceName, LocalDateTime.parse(lastBeat))
        return@withContext PersistenceAccessResult.result(if (heartbeat.lastBeat.isBefore(since)) null else heartbeat)
    }

    override suspend fun fetchHeartbeats(since: LocalDateTime): PersistenceAccessResult<List<Heartbeat>> = withContext(Dispatchers.IO) {
        val heartbeatKeys = redisClient.scanKeys(config.heartbeatPattern).toTypedArray()
            .ifEmpty { return@withContext PersistenceAccessResult.result(emptyList()) }
        val plainHeartbeats = redisClient.mget(heartbeatKeys).get()?.toList()?.zip(heartbeatKeys) ?: run { return@withContext PersistenceAccessResult.notFound() }
        val filteredHeartbeats = plainHeartbeats
            .map { beat -> Heartbeat(config.extractInstanceName(beat.second), LocalDateTime.parse(beat.first)) }
            .filter { !it.lastBeat.isBefore(since) }
        PersistenceAccessResult.result(filteredHeartbeats)
    }

    override suspend fun allJobsWithStatus(status: JobStatus): PersistenceAccessResult<List<Job>> =
        logTime(logger, Level.TRACE, { "Fetching all jobs with $status in $it" }) {
            getAllJobsBy(status, config.jobPattern)
        }

    override suspend fun allJobsOfInstance(status: JobStatus, instance: String): PersistenceAccessResult<List<Job>> =
        logTime(logger, Level.TRACE, { "Fetching all jobs of $status and instance $instance in $it" }) {
            getAllJobsBy(listOf("status", "executingInstance")) {
                it["status"] == status.toString() && it["executingInstance"] == instance
            }
        }

    override suspend fun allJobsFinishedBefore(date: LocalDateTime): PersistenceAccessResult<List<Job>> =
        logTime(logger, Level.TRACE, { "Fetching all jobs finished before $date in $it" }) {
            getAllJobsBy(listOf("status", "finishedAt")) {
                it["status"] in setOf(JobStatus.SUCCESS.toString(), JobStatus.FAILURE.toString(), JobStatus.CANCELLED.toString())
                        && it["finishedAt"]?.let(LocalDateTime::parse)?.isBefore(date) ?: false
            }
        }

    override suspend fun allJobsExceedingDbJobCount(maxNumberKeptJobs: Int): PersistenceAccessResult<List<Job>> {
        val overallKeyCount = redisClient.scanKeys(config.jobPattern).count()
        val exceedingJobCount = overallKeyCount - maxNumberKeptJobs
        return if (exceedingJobCount > 0) {
            logTime(logger, Level.TRACE, { "Fetched jobs for job count in $it" }) {
                getAllJobsBy(listOf("status")) {
                    it["status"] in setOf(JobStatus.SUCCESS.toString(), JobStatus.FAILURE.toString(), JobStatus.CANCELLED.toString())
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
                redisClient.hget(config.jobKey(uuid), "status").get()?.let { status -> JobStatus.valueOf(status) }
                    ?: return@fetchStates PersistenceAccessResult.uuidNotFound(uuid)
            })
    }

    private suspend fun getAllJobsBy(
        relevantJobFields: List<String>? = null,
        condition: ((Map<String, String?>) -> Boolean)? = null
    ): PersistenceAccessResult<List<Job>> {
        val allJobKeys = redisClient.scanKeys(config.jobPattern)
        val pipeline = Batch(false)
        // early exit (allJobKeys empty) because redisClient.exec throws exception if pipeline is empty
        val relevantKeys = if (relevantJobFields != null && condition != null && allJobKeys.isNotEmpty()) {
            allJobKeys.map { it to pipeline.hmget(it, relevantJobFields.toTypedArray()) }
            val keyQueries: List<List<String?>> = redisClient.exec(pipeline, true).get().asListOfNullableStringLists()
            allJobKeys.zip(keyQueries.map { relevantJobFields.zip(it).toMap() })
                .filter { condition(it.second) }.map { it.first }
        } else {
            allJobKeys
        }

        if (relevantKeys.isEmpty()) return PersistenceAccessResult.result(emptyList()) // early exit because redisClient.exec throws exception if pipeline is empty
        val pipelineJobs = Batch(false)
        relevantKeys.forEach { pipelineJobs.hgetall(it) }
        val jobs: List<Map<String, String>> = redisClient.exec(pipelineJobs, true).get().asListOfStringMaps()
        val result = relevantKeys.zip(jobs).map { it.second.redisMapToJob(config.extractUuid(it.first)) }
            .unwrapOrReturnFirstError { return@getAllJobsBy it }
        return result
    }

    private suspend fun GlideClient.scanKeys(keyPattern: String): List<String> =
        // we use a mutex to avoid interferences to the redisClient instance during iterative scan calls
        redisClientMutex.withLock {
            logTime(logger, Level.TRACE, { "Scanned keys of pattern $keyPattern in $it" }) {
                val options = ScanOptions.builder().type(ScanOptions.ObjectType.STRING).matchPattern(keyPattern).count(config.scanLimit).build()
                var cursor = "0"
                val keys: MutableList<String> = ArrayList()
                do {
                    val result = scan(cursor, options).get()
                    cursor = result[0] as String // first element is next cursor
                    val foundKeys = result[1].asStringList() // second are the found keys as string array
                    keys.addAll(foundKeys)
                } while (cursor != "0")
                return keys
            }
        }

    private fun getAllJobsBy(status: JobStatus, jobPattern: String): PersistenceAccessResult<List<Job>> {
        val valueArguments: List<String> = listOf(status.name, jobPattern, config.scanLimit.toString())
        val scriptOptions = ScriptOptions.builder().args(valueArguments).build()
        val jobs: List<List<String>> = redisClient.invokeScript(Script(LuaScripts.filterJobs, false), scriptOptions).get().asListOfStringLists()
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
    protected val batch: Batch,
    protected val config: RedisConfig
) : JobTransactionalPersistence {
    override suspend fun persistJob(job: Job): PersistenceAccessResult<Unit> {
        batch.hset(config.jobKey(job.uuid), job.toRedisMap())
        return PersistenceAccessResult.success
    }

    override suspend fun updateJob(job: Job): PersistenceAccessResult<Unit> {
        persistJob(job)
        job.nullFields().takeIf { it.isNotEmpty() }?.let { batch.hdel(config.jobKey(job.uuid), it.toTypedArray()) }
        return PersistenceAccessResult.success
    }

    override suspend fun deleteForUuid(uuid: String, persistencesPerType: Map<String, DataPersistence<*, *>>): PersistenceAccessResult<Unit> {
        batch.del(arrayOf(config.jobKey(uuid), config.inputKey(uuid), config.resultKey(uuid), config.failureKey(uuid)))
        return PersistenceAccessResult.success
    }
}

/**
 * [DataPersistence] implementation for Redis based on [Lettuce](https://lettuce.io).
 * It requires a [GlideClient] providing the connection to the Redis instance, serializers
 * and deserializers for job inputs and results, and a [Redis configuration][config].
 */
open class RedisDataPersistence<INPUT, RESULT>(
    redisClient: GlideClient,
    protected val inputSerializer: (INPUT) -> ByteArray,
    protected val resultSerializer: (RESULT) -> ByteArray,
    protected val inputDeserializer: (ByteArray) -> INPUT,
    protected val resultDeserializer: (ByteArray) -> RESULT,
    config: RedisConfig = DefaultRedisConfig(),
) : RedisJobPersistence(redisClient, config), DataPersistence<INPUT, RESULT> {

    override suspend fun transaction(block: suspend JobTransactionalPersistence.() -> Unit): PersistenceAccessResult<Unit> = dataTransaction(block)

    override suspend fun <T> dataTransaction(block: suspend DataTransactionalPersistence<INPUT, RESULT>.() -> T): PersistenceAccessResult<T> {
        val transaction = Batch(true)
        return try {
            RedisDataTransactionalPersistence(transaction, inputSerializer, resultSerializer, config)
                .run { block() }
                .also {
                    // The order here is relevant for storing new jobs. If we finish the string commands transaction first,
                    // other instances/threads may find the new job but not its input (especially since inputs may be quite large).
                    redisClient.exec(transaction, true)
                }.let { PersistenceAccessResult.result(it) }
        } catch (exception: Exception) {
            // TODO this may fail if one transaction already succeeded
            handleTransactionException(exception)
        }
    }

    override suspend fun fetchInput(uuid: String): PersistenceAccessResult<INPUT> = withContext(Dispatchers.IO) {
        redisClient.get(gs(config.inputKey(uuid))).get()
            ?.let { PersistenceAccessResult.result(inputDeserializer(it.bytes.decompress())) }
            ?: PersistenceAccessResult.uuidNotFound(uuid)
    }

    override suspend fun fetchResult(uuid: String): PersistenceAccessResult<RESULT> = withContext(Dispatchers.IO) {
        redisClient.get(gs(config.resultKey(uuid))).get()
            ?.let { PersistenceAccessResult.result(resultDeserializer(it.bytes.decompress())) }
            ?: PersistenceAccessResult.uuidNotFound(uuid)
    }

    override suspend fun fetchFailure(uuid: String): PersistenceAccessResult<String> = withContext(Dispatchers.IO) {
        redisClient.get(config.failureKey(uuid)).get()
            ?.let { PersistenceAccessResult.result(it) }
            ?: PersistenceAccessResult.uuidNotFound(uuid)
    }

    private fun ByteArray.decompress(): ByteArray =
        if (config.useCompression) GZIPInputStream(ByteArrayInputStream(this)).use { gz -> gz.readAllBytes() } else this
}

/**
 * [DataTransactionalPersistence] implementation for Redis.
 *
 * Note that all methods will always return `PersistenceAccessResult.success`, since commands within transaction are not yet
 * executed, so the only real kind of error would be connection problems which are ok to be caught in [RedisDataPersistence.dataTransaction].
 */
open class RedisDataTransactionalPersistence<INPUT, RESULT>(
    batch: Batch,
    protected val inputSerializer: (INPUT) -> ByteArray,
    protected val resultSerializer: (RESULT) -> ByteArray,
    config: RedisConfig
) : RedisJobTransactionalPersistence(batch, config), DataTransactionalPersistence<INPUT, RESULT> {

    override suspend fun persistInput(job: Job, input: INPUT): PersistenceAccessResult<Unit> {
        batch.set(gs(config.inputKey(job.uuid).toByteArray()), gs(inputSerializer(input).compress()))
        return PersistenceAccessResult.success
    }

    override suspend fun persistOrUpdateResult(job: Job, result: RESULT): PersistenceAccessResult<Unit> {
        batch.set(gs(config.resultKey(job.uuid).toByteArray()), gs(resultSerializer(result).compress()))
        return PersistenceAccessResult.success
    }

    override suspend fun persistOrUpdateFailure(job: Job, failure: String): PersistenceAccessResult<Unit> {
        batch.set(config.failureKey(job.uuid), failure)
        return PersistenceAccessResult.success
    }

    private fun ByteArray.compress(): ByteArray =
        if (config.useCompression) ByteArrayOutputStream().also { GZIPOutputStream(it).use { gz -> gz.write(this) } }.toByteArray() else this
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

private fun Any.asStringList(): List<String> = (this as Array<Object>).map { it as String }

private fun Any.asListOfStringLists(): List<List<String>> = (this as Array<Object>).map { elt -> (elt as Array<*>).map { it as String } }

private fun Any.asListOfNullableStringLists(): List<List<String?>> = (this as Array<Object>).map { elt -> (elt as Array<*>).map { it as String? } }

private fun Any.asListOfStringMaps(): List<Map<String, String>> = (this as Array<Object>).map { it as Map<String, String> }
