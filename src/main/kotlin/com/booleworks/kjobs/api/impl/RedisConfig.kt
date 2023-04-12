// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.api.impl

/**
 * Configuration for the [RedisDataPersistence].
 */
interface RedisConfig {
    val jobPattern: String
        get() = "*:job"
    val heartbeatPattern: String
        get() = "*:heartbeat"

    fun jobKey(uuid: String): String = "$uuid:job"
    fun inputKey(uuid: String): String = "$uuid:input"
    fun resultKey(uuid: String): String = "$uuid:result"
    fun heartbeatKey(instanceName: String): String = "$instanceName:heartbeat"

    fun extractUuid(jobKey: String): String = jobKey.split(":")[0]
    fun extractInstanceName(heartbeatKey: String): String = heartbeatKey.split(":")[0]
}

/**
 * Default Redis configuration.
 */
open class DefaultRedisConfig : RedisConfig
