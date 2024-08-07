// SPDX-License-Identifier: MIT
// Copyright 2023-2024 BooleWorks GmbH

package com.booleworks.kjobs.api.persistence.redis

/**
 * Configuration for the [RedisDataPersistence].
 */
interface RedisConfig {
    val useCompression: Boolean get() = false

    val jobPattern: String get() = "job:*"
    val heartbeatPattern: String get() = "heartbeat:*"

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
