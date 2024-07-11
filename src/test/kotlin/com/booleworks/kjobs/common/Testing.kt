// SPDX-License-Identifier: MIT
// Copyright 2023-2024 BooleWorks GmbH

package com.booleworks.kjobs.common

import com.github.fppt.jedismock.RedisServer
import io.kotest.core.spec.style.FunSpec
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking
import kotlin.time.Duration.Companion.minutes
import kotlin.time.toJavaDuration

fun FunSpec.testBlocking(name: String, block: suspend CoroutineScope.() -> Unit) = test(name) {
    runBlocking {
        block()
    }
}

val RedisServer.lettuceClient: RedisClient get() = RedisClient.create(RedisURI(host, bindPort, 1.minutes.toJavaDuration()))
