package com.mercedesbenz.jobframework.data

import com.mercedesbenz.jobframework.util.Either

typealias PersistenceAccessResult<R> = Either<PersistenceAccessError, R>

inline fun <R> PersistenceAccessResult<R>.orQuitWith(block: (PersistenceAccessError) -> Nothing): R {
    return when (this) {
        is Either.Right -> value
        is Either.Left -> block(value)
    }
}

val <R> PersistenceAccessResult<R>.successful: Boolean
    get() = this is Either.Right

inline fun <R> PersistenceAccessResult<R>.ifError(block: (PersistenceAccessError) -> Unit) {
    onLeft { block(it) }
}

fun <R, T> PersistenceAccessResult<R>.mapResult(mapper: (R) -> T): PersistenceAccessResult<T> = this.map(mapper)

val Either.Companion.success: Either.Right<Unit>
    get() = Either.Right(Unit)

fun <R> Either.Companion.result(result: R): PersistenceAccessResult<R> = Either.Right(result)
fun <R> Either.Companion.notFound(): PersistenceAccessResult<R> = Either.Left(PersistenceAccessError.NotFound)
fun <R> Either.Companion.internalError(message: String): PersistenceAccessResult<R> = Either.Left(PersistenceAccessError.InternalError(message))

sealed interface PersistenceAccessError {
    object NotFound : PersistenceAccessError {
        override fun toString() = "Not found"
    }

    data class InternalError(val message: String) : PersistenceAccessError {
        override fun toString() = message
    }
}
