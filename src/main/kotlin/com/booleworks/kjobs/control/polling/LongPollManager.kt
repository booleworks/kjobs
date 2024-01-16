package com.booleworks.kjobs.control.polling

import com.booleworks.kjobs.control.polling.NopLongPollManager.subscribe
import com.booleworks.kjobs.data.JobStatus
import com.booleworks.kjobs.data.PollStatus
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlin.time.Duration

/**
 * Interface for classes which can handle the "backend mechanism" of long polls.
 *
 * A long poll manager implements a mechanism which is very similar to the publish-subscribe pattern.
 * It must offer the possibility to publish [successful][publishSuccess] and [unsuccessful][publishFailure]
 * results and pass this information to all subscribers.
 *
 * **Note that the implementation is not trivial in applications with multiple instances**.
 * A naive implementation for multiple instances would be to constantly query the persistence layer
 * for updates on the subscribed UUIDs.
 * The [RedisLongPollManager] uses the pub/sub mechanism of Redis for the implementation of this task.
 */
interface LongPollManager {
    /**
     * This method is called when the job with the given [uuid] was successfully computed, i.e. is now in status
     * [JobStatus.SUCCESS] and the result is ready to be retrieved.
     * The implementing method should pass this information to all [subscribed][subscribe] clients, preferably
     * (but depending on the implementation concept) on all instances of the application.
     */
    fun publishSuccess(uuid: String)

    /**
     * This method is called when the job with the given [uuid] was computed with error, i.e. is now in status
     * [JobStatus.FAILURE] and the error message is ready to be retrieved.
     * The implementing method should pass this information to all [subscribed][subscribe] clients, preferably
     * (but depending on the implementation concept) on all instances of the application.
     */
    fun publishFailure(uuid: String)

    /**
     * Allows the caller to subscribe to a [uuid]. The result should be a [deferred value][Deferred] providing
     * a [PollStatus]. If [publishSuccess] with the respective [uuid] is called within the given [timeout],
     * the deferred value should return [PollStatus.SUCCESS]. If [publishFailure] with the respective [uuid]
     * is called within the given [timeout], the deferred value should return [PollStatus.FAILURE].
     *
     * If no relevant publish call is encountered within the given timeout, the deferred value should return
     * [PollStatus.TIMEOUT].
     *
     * The method is expected to return quickly. Especially it should not wait for the corresponding `publish`
     * call for the uuid. Instead, the caller must [wait for][Deferred.await] the poll to finish.
     *
     * The [CoroutineScope] context object can be used to launch the coroutines required for waiting for the
     * result.
     *
     * Note that cleanup at the end of the call can be scheduled using [Deferred.invokeOnCompletion].
     *
     * If the subscription is not required anymore, the returned Deferred object will be
     * [cancelled][kotlinx.coroutines.cancelAndJoin].
     *
     * Returning [PollStatus.ABORTED] is not intended right now, since it is only used if the status of the
     * job is [JobStatus.CANCELLED] or [JobStatus.CANCEL_REQUESTED] at the beginning of the poll.
     */
    fun CoroutineScope.subscribe(uuid: String, timeout: Duration): Deferred<PollStatus>
}

/**
 * Dummy implementation of a long poll manager. It ignores publish calls and throws an error on [subscribe]
 * calls, thus it is used as default implementation when long polling is not enabled (which has the advantage
 * that we can publish results whether or not long polling is enabled).
 */
internal object NopLongPollManager : LongPollManager {
    override fun publishSuccess(uuid: String) {
        // Do nothing
    }

    override fun publishFailure(uuid: String) {
        // Do nothing
    }

    override fun CoroutineScope.subscribe(uuid: String, timeout: Duration): Deferred<PollStatus> {
        error("Long Polling not supported")
    }
}
