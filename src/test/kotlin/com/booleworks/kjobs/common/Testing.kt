// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.common

import io.kotest.core.spec.style.FunSpec
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking

fun FunSpec.testBlocking(name: String, block: suspend CoroutineScope.() -> Unit) = test(name) {
    runBlocking {
        block()
    }
}
