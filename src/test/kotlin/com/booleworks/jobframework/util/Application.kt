// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.jobframework.util

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.github.fppt.jedismock.RedisServer
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

fun testJobFramework(block: suspend ApplicationTestBuilder.() -> Unit) = testApplication {
    defaultRedis = RedisServer.newRedisServer().start()
//    environment {
//        developmentMode = false
//        config = ApplicationConfig("src/test/resources/application.conf")
////            .mergeWith(MapApplicationConfig("ktor.environment.redisUrl" to "http://${redis.host}:${redis.bindPort}"))
//    }
    block()
    defaultRedis.stop()
}
