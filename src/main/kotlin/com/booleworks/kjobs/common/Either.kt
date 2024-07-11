// SPDX-License-Identifier: MIT
// Copyright 2023-2024 BooleWorks GmbH

package com.booleworks.kjobs.common

import com.booleworks.kjobs.common.Either.Left
import com.booleworks.kjobs.common.Either.Right
import com.booleworks.kjobs.data.PersistenceAccessResult

/**
 * An implementation of Either, inspired by [Arrow](https://arrow-kt.io/).
 * It holds either a [Left] or a [Right] value.
 *
 * Can especially be used for computation results where, by convention, [Left]
 * would hold an error and [Right] would hold the result.
 *
 * The type alias [PersistenceAccessResult] is defined base of this class.
 */
sealed class Either<out L, out R> {
    /**
     * Whether this is a [Left] value or not (if not, it is a [Right] value).
     */
    abstract val isLeft: Boolean

    /**
     * Whether this is a [Right] value of not (if not, it is a [Left] value).
     */
    abstract val isRight: Boolean

    /**
     * A Left value.
     */
    data class Left<out L>(val value: L) : Either<L, Nothing>() {
        override val isLeft = true
        override val isRight = false
    }

    /**
     * A Right value.
     */
    data class Right<out R>(val value: R) : Either<Nothing, R>() {
        override val isLeft = false
        override val isRight = true
    }

    /**
     * Applies the given function to the right value and does nothing if this is a left value.
     */
    inline fun <T> map(fl: (L) -> T, fr: (R) -> T): T = when (this) {
        is Left -> fl(value)
        is Right -> fr(value)
    }

    /**
     * Applies the given function to the left value and does nothing if this is a right value.
     */
    inline fun <T> mapLeft(f: (L) -> T): Either<T, R> = when (this) {
        is Left -> Left(f(value))
        is Right -> this
    }

    /**
     * Applies the given function to the right value and does nothing if this is a left value.
     */
    inline fun <T> mapRight(f: (R) -> T): Either<L, T> = when (this) {
        is Left -> this
        is Right -> Right(f(value))
    }

    /**
     * Performs the given action on the left value, if it is present.
     * Returns the original object.
     */
    inline fun onLeft(action: (L) -> Unit): Either<L, R> = this.apply {
        if (this is Left) action(value)
    }

    /**
     * Performs the given action on the right value, if it is present.
     * Returns the original object.
     */
    inline fun onRight(action: (R) -> Unit): Either<L, R> = this.apply {
        if (this is Right) action(value)
    }

    /**
     * Returns the right value or executes the given block with the left value which will not return.
     * This can be used to throw exceptions or return to the outer function.
     */
    inline fun rightOr(block: (L) -> Nothing): R = when (this) {
        is Right -> value
        is Left -> block(value)
    }

    /**
     * Empty companion object to allow extension functions on it like `Either.Companion.result`.
     */
    companion object
}

/**
 * Returns the right value if it is present, otherwise applies the given function to the left value.
 */
inline fun <L, R> Either<L, R>.getOrElse(f: (L) -> R): R = when (this) {
    is Left -> f(value)
    is Right -> value
}

/**
 * Unwraps all [results][R] of this collection. If any element is a Left (error) value the first of them is
 * passed to [onError] s.t. this function does not return anymore.
 */
inline fun <reified L, R> Iterable<Either<L, R>>.unwrapOrReturnFirstError(onError: (Left<L>) -> Nothing): Right<List<R>> =
    Right(map { (it as? Right<R>)?.value ?: onError(it as Left<L>) })
