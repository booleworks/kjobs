// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.data

import com.booleworks.kjobs.api.persistence.DataPersistence
import com.booleworks.kjobs.api.persistence.DataTransactionalPersistence
import com.booleworks.kjobs.common.Either
import com.booleworks.kjobs.data.PersistenceAccessError.InternalError
import com.booleworks.kjobs.data.PersistenceAccessError.NotFound

/**
 * The result of an interaction with [DataPersistence] or [DataTransactionalPersistence].
 * In case of a successful access it contains the result of type `R`, otherwise
 * it contains a [PersistenceAccessError].
 */
typealias PersistenceAccessResult<R> = Either<PersistenceAccessError, R>

/**
 * Interface of a persistence access error. Has exactly two implementations: [NotFound] and [InternalError].
 *
 * The [message] contains more information about the error.
 */
sealed interface PersistenceAccessError {
    val message: String

    /**
     * Indicates that a persistence access failed because the requested entity was not found.
     */
    object NotFound : PersistenceAccessError {
        override val message = "Not found"
        override fun toString() = message
    }

    /**
     * Indicates that an update was not performed since the data has been modified by someone else.
     */
    object Modified : PersistenceAccessError {
        override val message = "The update was not performed because the data to update has been altered by someone else"
        override fun toString() = message
    }

    /**
     * Indicates that a persistence access failed because the item with id `uuid` was not found.
     */
    data class UuidNotFound(val uuid: String) : PersistenceAccessError {
        override val message = "UUID not found: $uuid"
        override fun toString() = message
    }

    /**
     * Indicates that a persistence access failed because of an internal error (e.g. a connection problem).
     */
    data class InternalError(override val message: String) : PersistenceAccessError {
        override fun toString() = message
    }
}

/**
 * Returns the result of the persistence access or, in case of a failure, executes the given block which
 * must not return (i.e. it must throw an exception or return directly to the calling function).
 */
inline fun <R> PersistenceAccessResult<R>.orQuitWith(block: (PersistenceAccessError) -> Nothing): R {
    return when (this) {
        is Either.Right -> value
        is Either.Left -> block(value)
    }
}

/**
 * Executes the given block in case of an error.
 */
inline fun <R> PersistenceAccessResult<R>.ifError(block: (PersistenceAccessError) -> Unit) {
    onLeft { block(it) }
}

/**
 * Maps the result using the given mapping function.
 */
inline fun <R, T> PersistenceAccessResult<R>.mapResult(mapper: (R) -> T): PersistenceAccessResult<T> = this.mapRight(mapper)

/**
 * Whether this persistence access was successful.
 */
val <R> PersistenceAccessResult<R>.successful: Boolean get() = this is Either.Right

/**
 * Returns an object indicating a successful persistence access.
 */
val Either.Companion.success: PersistenceAccessResult<Unit> get() = Either.Right(Unit)

/**
 * Returns an object containing the [result] of a successful persistence access.
 */
fun <R> Either.Companion.result(result: R): PersistenceAccessResult<R> = Either.Right(result)

/**
 * Returns an object indicating that the persistence access failed because the item was not found.
 */
fun <R> Either.Companion.notFound(): PersistenceAccessResult<R> = Either.Left(NotFound)

/**
 * Returns an object indicating that an update was not performed since the updated data was modified by
 * someone else.
 */
fun <R> Either.Companion.modified(): PersistenceAccessResult<R> = Either.Left(PersistenceAccessError.Modified)

/**
 * Returns an object indicating that the persistence access failed because the item with id [uuid] was not found.
 */
fun <R> Either.Companion.uuidNotFound(uuid: String): PersistenceAccessResult<R> = Either.Left(PersistenceAccessError.UuidNotFound(uuid))

/**
 * Returns an object indicating that the persistence access failed with a given error [message].
 */
fun <R> Either.Companion.internalError(message: String): PersistenceAccessResult<R> = Either.Left(InternalError(message))
