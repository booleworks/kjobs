// SPDX-License-Identifier: MIT
// Copyright 2023-2024 BooleWorks GmbH

package com.booleworks.kjobs.common

import io.kotest.assertions.fail
import io.kotest.assertions.throwables.shouldThrowWithMessage
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.equals.shouldBeEqual

class EitherTest : FunSpec({

    val left = Either.Left(42)
    val right = Either.Right("Hello World")

    test("test value") {
        left.value shouldBeEqual 42
        right.value shouldBeEqual "Hello World"
    }

    test("test isLeft") {
        left.isLeft shouldBeEqual true
        right.isLeft shouldBeEqual false
    }

    test("test isRight") {
        left.isRight shouldBeEqual false
        right.isRight shouldBeEqual true
    }

    test("test map") {
        left.map({ it / 2 }, {}) shouldBeEqual 21
        right.map({ }, { "$it!" }) shouldBeEqual "Hello World!"
    }

    test("test mapLeft") {
        left.mapLeft { it / 2 } shouldBeEqual Either.Left(21)
        right.mapLeft { } shouldBeEqual right
    }

    test("test mapRight") {
        left.mapRight {} shouldBeEqual left
        right.mapRight { "$it!" } shouldBeEqual Either.Right("Hello World!")
    }

    test("test onLeft") {
        var executed = false
        left.onLeft { executed = true } shouldBeEqual left
        executed shouldBeEqual true
        right.onLeft { fail("Should not be executed") } shouldBeEqual right
    }

    test("test onRight") {
        left.onRight { fail("Should not be executed") } shouldBeEqual left
        var executed = false
        right.onRight { executed = true } shouldBeEqual right
        executed shouldBeEqual true
    }

    test("test rightOr") {
        shouldThrowWithMessage<IllegalArgumentException>("It worked") { left.rightOr { throw IllegalArgumentException("It worked") } }
        right.rightOr { fail("Should not be executed") } shouldBeEqual "Hello World"
    }

    test("test getOrElse") {
        left.getOrElse { "foo $it" } shouldBeEqual "foo 42"
        right.getOrElse { fail("Should not be executed") } shouldBeEqual "Hello World"
    }

    test("test unwrapOrReturnFirstError") {
        listOf(42, 23, 35, 46).map { Either.Right(it) }.unwrapOrReturnFirstError<Any, Int> { fail("Should not be executed") }
        shouldThrowWithMessage<IllegalArgumentException>("Found problem: Hello") {
            listOf(Either.Right(42), Either.Left("Hello"), Either.Right(23), Either.Left("World"))
                .unwrapOrReturnFirstError { throw IllegalArgumentException("Found problem: ${it.value}") }
        }
    }
})
