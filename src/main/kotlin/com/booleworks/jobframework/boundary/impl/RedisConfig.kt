// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.jobframework.boundary.impl

/**
 * Configuration for the [RedisPersistence].
 */
interface RedisConfig {
    val jobPattern: String
        get() = "*:job"

    fun jobKey(uuid: String): String = "$uuid:job"
    fun inputKey(uuid: String): String = "$uuid:input"
    fun resultKey(uuid: String): String = "$uuid:result"

    fun extractUuid(key: String): String = key.split(":")[0]
}

/**
 * Default Redis configuration.
 */
open class DefaultRedisConfig : RedisConfig
