// SPDX-License-Identifier: MIT
// Copyright 2023-2024 BooleWorks GmbH

package com.booleworks.kjobs.data

import com.booleworks.kjobs.api.InputValidationResult
import com.booleworks.kjobs.api.persistence.DataPersistence
import com.booleworks.kjobs.control.polling.LongPollManager
import io.ktor.server.routing.Route
import io.ktor.server.routing.RoutingContext
import kotlin.time.Duration

internal class ApiConfig<INPUT, RESULT>(
    val inputReceiver: suspend RoutingContext.() -> INPUT,
    val resultResponder: suspend RoutingContext.(RESULT) -> Unit,
    val inputValidation: (INPUT) -> InputValidationResult,
    val enableDeletion: Boolean,
    val enableCancellation: Boolean,
    val syncMockConfig: SynchronousResourceConfig<INPUT>,
    val jobInfoConfig: JobInfoConfig,
    val longPollingConfig: LongPollingConfig,
    val submitRoute: Route.(suspend RoutingContext.() -> Unit) -> Unit,
    val statusRoute: Route.(suspend RoutingContext.() -> Unit) -> Unit,
    val resultRoute: Route.(suspend RoutingContext.() -> Unit) -> Unit,
    val failureRoute: Route.(suspend RoutingContext.() -> Unit) -> Unit,
    val deleteRoute: Route.(suspend RoutingContext.() -> Unit) -> Unit,
    val cancelRoute: Route.(suspend RoutingContext.() -> Unit) -> Unit,
    val syncRoute: Route.(suspend RoutingContext.() -> Unit) -> Unit,
    val infoRoute: Route.(suspend RoutingContext.() -> Unit) -> Unit,
    val longPollingRoute: Route.(suspend RoutingContext.() -> Unit) -> Unit,
)

internal data class JobConfig<INPUT, RESULT>(
    val jobType: String,
    val persistence: DataPersistence<INPUT, RESULT>,
    val myInstanceName: String,
    val tagProvider: (INPUT) -> List<String>,
    val customInfoProvider: (INPUT) -> String?,
    val priorityProvider: (INPUT) -> Int,
)

internal class SynchronousResourceConfig<INPUT>(
    val enabled: Boolean,
    val checkInterval: Duration,
    val maxWaitingTime: Duration,
    val priorityProvider: (INPUT) -> Int,
)

internal class JobInfoConfig(
    val enabled: Boolean,
    val responder: suspend RoutingContext.(Job) -> Unit
)

internal class LongPollingConfig(
    val enabled: Boolean,
    val longPollManager: () -> LongPollManager,
    val connectionTimeout: Duration,
)
