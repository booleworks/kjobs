// SPDX-License-Identifier: MIT
// Copyright 2023-2024 BooleWorks GmbH

package com.booleworks.kjobs.api.persistence.redis

import kotlin.time.Duration
import kotlin.time.Duration.Companion.days

/**
 * Configuration for the [RedisDataPersistence].
 */
interface RedisConfig {
    val useCompression: Boolean get() = false

    val jobPattern: String get() = "job:*"
    val heartbeatPattern: String get() = "heartbeat:*"

    /**
     * The heartbeat expiration duration.
     *
     * A heartbeat has an expiration to prevent heartbeat entries of previous instances to remain forever in Redis.
     */
    val heartbeatExpiration: Duration get() = 1.days

    /**
     * The limit that is used for the Redis scan operations.
     *
     * For most use cases the default value is good. For high performance use cases it can help to
     * adjust the limit.
     */
    val scanLimit: Long get() = 1000L

    fun jobKey(uuid: String): String = "job:$uuid"
    fun inputKey(uuid: String): String = "input:$uuid"
    fun resultKey(uuid: String): String = "result:$uuid"
    fun failureKey(uuid: String): String = "failure:$uuid"
    fun heartbeatKey(instanceName: String): String = "heartbeat:$instanceName"

    fun extractUuid(jobKey: String): String = jobKey.split(":")[1]
    fun extractInstanceName(heartbeatKey: String): String = heartbeatKey.split(":")[1]
}

/**
 * Default Redis configuration.
 */
open class DefaultRedisConfig : RedisConfig
