// SPDX-License-Identifier: MIT
// Copyright 2023-2024 BooleWorks GmbH

package com.booleworks.kjobs.common

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.github.fppt.jedismock.RedisServer
import io.kotest.core.spec.style.FunSpec
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication

fun Application.module() {
    install(ContentNegotiation) {
        jackson {
            enable(SerializationFeature.INDENT_OUTPUT)
            disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            registerKotlinModule()
            registerModule(JavaTimeModule())
        }
    }
}

lateinit var defaultRedis: RedisServer

fun FunSpec.testJobFrameworkWithRedis(testName: String, block: suspend ApplicationTestBuilder.() -> Unit) = test(testName) {
    testApplication {
        defaultRedis = RedisServer.newRedisServer().start()
        block()
//    defaultRedis.stop()
    }
}
