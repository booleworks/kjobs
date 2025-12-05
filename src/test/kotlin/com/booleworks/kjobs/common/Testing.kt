// SPDX-License-Identifier: MIT
// Copyright 2023-2024 BooleWorks GmbH

package com.booleworks.kjobs.common

import com.github.fppt.jedismock.RedisServer
import glide.api.GlideClient
import glide.api.models.configuration.GlideClientConfiguration
import glide.api.models.configuration.NodeAddress
import glide.api.models.configuration.ProtocolVersion
import io.kotest.core.spec.style.FunSpec
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking

fun FunSpec.testBlocking(name: String, block: suspend CoroutineScope.() -> Unit) = test(name) {
    runBlocking {
        block()
    }
}

val RedisServer.glideClientConfigBuilder: GlideClientConfiguration.GlideClientConfigurationBuilder<*, *>
    get() {
        return GlideClientConfiguration.builder()
            .address(NodeAddress.builder().host("localhost").port(bindPort).build())
            .useTLS(false)
            .requestTimeout(60_000) // 1 minute
            .protocol(ProtocolVersion.RESP2) // Jedis Mock does not support RESP3
    }

val RedisServer.glideClient: GlideClient
    get() {
        val config = glideClientConfigBuilder.build()
        return GlideClient.createClient(config).get()
    }
