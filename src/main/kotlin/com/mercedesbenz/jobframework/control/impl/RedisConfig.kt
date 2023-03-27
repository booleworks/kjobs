package com.mercedesbenz.jobframework.control.impl

import com.mercedesbenz.jobframework.data.Job

interface RedisConfig {
    val jobPattern: String
        get() = "*:job"

    fun jobKey(uuid: String): String = "$uuid:job"
    fun inputKey(uuid: String): String = "$uuid:input"
    fun resultKey(uuid: String): String = "$uuid:result"
}

open class DefaultRedisConfig: RedisConfig
