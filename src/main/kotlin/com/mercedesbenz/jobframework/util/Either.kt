package com.mercedesbenz.jobframework.util

import com.mercedesbenz.jobframework.util.Either.Left
import com.mercedesbenz.jobframework.util.Either.Right

/**
 * An implementation of Either, inspired by [Arrow](https://arrow-kt.io/)
 *
 * Can especially be used for computation results where, by convention, [Left]
 * would hold an error and [Right] would hold the result.
 */
sealed class Either<out L, out R> {
    abstract val isLeft: Boolean
    abstract val isRight: Boolean

    data class Left<out L>(val value: L) : Either<L, Nothing>() {
        override val isLeft = true
        override val isRight = false
    }

    data class Right<out R>(val value: R) : Either<Nothing, R>() {
        override val isLeft = false
        override val isRight = true
    }

    /**
     * Applies the given function to the right value and does nothing if this is a left value.
     */
    inline fun <T> map(f: (R) -> T): Either<L, T> = when (this) {
        is Left -> this
        is Right -> Right(f(value))
    }

    /**
     * Applies the given function to the left value and does nothing if this is a right value.
     */
    inline fun <T> mapLeft(f: (L) -> T): Either<T, R> = when (this) {
        is Left -> Left(f(value))
        is Right -> this
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
     * Empty companion object to allow extension functions on it like [Either.Companion.result].
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
