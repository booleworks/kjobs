// SPDX-License-Identifier: MIT
// Copyright 2023-2024 BooleWorks GmbH

package com.booleworks.kjobs.api.persistence.hashmap

import com.booleworks.kjobs.api.persistence.DataPersistence
import com.booleworks.kjobs.api.persistence.DataTransactionalPersistence
import com.booleworks.kjobs.api.persistence.JobPersistence
import com.booleworks.kjobs.api.persistence.JobTransactionalPersistence
import com.booleworks.kjobs.common.Either
import com.booleworks.kjobs.data.Heartbeat
import com.booleworks.kjobs.data.Job
import com.booleworks.kjobs.data.JobStatus
import com.booleworks.kjobs.data.PersistenceAccessResult
import com.booleworks.kjobs.data.modified
import com.booleworks.kjobs.data.result
import com.booleworks.kjobs.data.success
import com.booleworks.kjobs.data.uuidNotFound
import java.time.LocalDateTime
import java.util.concurrent.ConcurrentHashMap

/**
 * The most simple [JobPersistence] implementation which just uses an internal [ConcurrentHashMap].
 *
 * This implementation should only be used for testing in environments with a *single* instance,
 * otherwise using this implementation does not make sense.
 */
open class HashMapJobPersistence : JobPersistence, JobTransactionalPersistence {
    protected val jobs = ConcurrentHashMap<String, Job>()
    protected var latestHeartbeat = Heartbeat("", LocalDateTime.now()) // dummy value

    override suspend fun transaction(block: suspend JobTransactionalPersistence.() -> Unit): PersistenceAccessResult<Unit> =
        PersistenceAccessResult.success.also { block(this) }

    override suspend fun transactionWithPreconditions(
        preconditions: Map<String, (Job) -> Boolean>,
        block: suspend JobTransactionalPersistence.() -> Unit
    ): PersistenceAccessResult<Unit> {
        preconditions.forEach { (uuid, condition) ->
            jobs[uuid]?.also { if (!condition(it)) return PersistenceAccessResult.modified() } ?: run { return PersistenceAccessResult.uuidNotFound(uuid) }
        }
        return transaction(block)
    }

    override suspend fun fetchAllJobs(): PersistenceAccessResult<List<Job>> = PersistenceAccessResult.result(jobs.values.map { it.copy() })

    override suspend fun fetchJob(uuid: String): PersistenceAccessResult<Job> =
        jobs[uuid]?.let { PersistenceAccessResult.result(it.copy()) } ?: PersistenceAccessResult.uuidNotFound(uuid)

    override suspend fun fetchHeartbeats(since: LocalDateTime): PersistenceAccessResult<List<Heartbeat>> =
        PersistenceAccessResult.result(if (latestHeartbeat.lastBeat.isBefore(since)) emptyList() else listOf(latestHeartbeat.copy()))

    override suspend fun allJobsWithStatus(status: JobStatus): PersistenceAccessResult<List<Job>> =
        PersistenceAccessResult.result(jobs.values.filter { it.status == status }.map { it.copy() })

    override suspend fun allJobsOfInstance(status: JobStatus, instance: String): PersistenceAccessResult<List<Job>> =
        PersistenceAccessResult.result(jobs.values.filter { it.status == status && it.executingInstance == instance }.map { it.copy() })

    override suspend fun allJobsFinishedBefore(date: LocalDateTime): PersistenceAccessResult<List<Job>> =
        PersistenceAccessResult.result(jobs.values.filter {
            it.status in setOf(
                JobStatus.SUCCESS,
                JobStatus.FAILURE,
                JobStatus.CANCELLED
            ) && it.finishedAt?.isBefore(date) ?: false
        }.map { it.copy() })

    override suspend fun allJobsExceedingDbJobCount(maxNumberKeptJobs: Int): PersistenceAccessResult<List<Job>> {
        val exceedingJobCount = jobs.count() - maxNumberKeptJobs
        return if (exceedingJobCount > 0) {
            jobs.values.filter {
                it.status in setOf(
                    JobStatus.SUCCESS,
                    JobStatus.FAILURE,
                    JobStatus.CANCELLED
                )
            }.let { finishedJobs ->
                PersistenceAccessResult.result(finishedJobs.sortedBy { job -> job.createdAt }.take(exceedingJobCount).map { it.copy() })
            }
        } else {
            PersistenceAccessResult.result(emptyList())
        }

    }


    override suspend fun persistJob(job: Job): PersistenceAccessResult<Unit> = PersistenceAccessResult.success.also { jobs[job.uuid] = job }

    override suspend fun updateJob(job: Job): PersistenceAccessResult<Unit> = PersistenceAccessResult.success.also { jobs[job.uuid] = job }

    override suspend fun deleteForUuid(uuid: String, persistencesPerType: Map<String, DataPersistence<*, *>>): PersistenceAccessResult<Unit> {
        jobs.remove(uuid)?.let { job ->
            (persistencesPerType[job.type] as? HashMapDataPersistence<*, *>)?.let {
                it.inputs.remove(uuid)
                it.results.remove(uuid)
                it.failures.remove(uuid)
            }
        }
        return PersistenceAccessResult.success
    }

    override suspend fun updateHeartbeat(heartbeat: Heartbeat): PersistenceAccessResult<Unit> {
        latestHeartbeat = heartbeat
        return PersistenceAccessResult.success
    }
}

/**
 * The most simple [DataPersistence] implementation which just uses internal [ConcurrentHashMap]s.
 *
 * This implementation should only be used for testing in environments with a *single* instance,
 * otherwise using this implementation does not make sense.
 */
class HashMapDataPersistence<INPUT, RESULT>(jobPersistence: HashMapJobPersistence) : DataPersistence<INPUT, RESULT>,
    DataTransactionalPersistence<INPUT, RESULT>, JobPersistence by jobPersistence, JobTransactionalPersistence by jobPersistence {
    internal val inputs = ConcurrentHashMap<String, INPUT>()
    internal val results = ConcurrentHashMap<String, RESULT>()
    internal val failures = ConcurrentHashMap<String, String>()

    override suspend fun <T> dataTransaction(block: suspend DataTransactionalPersistence<INPUT, RESULT>.() -> T): PersistenceAccessResult<T> =
        Either.Right(block(this))

    override suspend fun fetchInput(uuid: String): PersistenceAccessResult<INPUT> =
        inputs[uuid]?.let { PersistenceAccessResult.result(it) } ?: PersistenceAccessResult.uuidNotFound(uuid)

    override suspend fun fetchResult(uuid: String): PersistenceAccessResult<RESULT> =
        results[uuid]?.let { PersistenceAccessResult.result(it) } ?: PersistenceAccessResult.uuidNotFound(uuid)

    override suspend fun fetchFailure(uuid: String): PersistenceAccessResult<String> =
        failures[uuid]?.let { PersistenceAccessResult.result(it) } ?: PersistenceAccessResult.uuidNotFound(uuid)

    override suspend fun persistInput(job: Job, input: INPUT): PersistenceAccessResult<Unit> {
        inputs[job.uuid] = input
        return PersistenceAccessResult.success
    }

    override suspend fun persistOrUpdateResult(job: Job, result: RESULT): PersistenceAccessResult<Unit> {
        results[job.uuid] = result
        return PersistenceAccessResult.success
    }

    override suspend fun persistOrUpdateFailure(job: Job, failure: String): PersistenceAccessResult<Unit> {
        failures[job.uuid] = failure
        return PersistenceAccessResult.success
    }
}
