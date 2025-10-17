// SPDX-License-Identifier: MIT
// Copyright 2023-2024 BooleWorks GmbH

package com.booleworks.kjobs.control.polling

import com.booleworks.kjobs.control.logTime
import com.booleworks.kjobs.data.PollStatus
import io.lettuce.core.RedisClient
import io.lettuce.core.api.sync.RedisCommands
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.event.Level
import kotlin.concurrent.thread
import kotlin.time.Duration
import kotlin.time.toJavaDuration

private val log: Logger = LoggerFactory.getLogger("com.booleworks.kjobs.RedisLongPollManager")

private const val DEFAULT_REDIS_CHANNEL = "job_finished"

private const val SUCCESS_INDICATOR = "||SUCCESS"
private const val FAILURE_INDICATOR = "||FAILURE"

private const val CONNECTION_CLOSE_DELAY = 50L

class RedisLongPollManager(protected val redisClient: RedisClient, protected val channelName: String = DEFAULT_REDIS_CHANNEL) : LongPollManager {
    val stringCommands: RedisCommands<String, String> = redisClient.connect().sync()

    override fun publishSuccess(uuid: String) = publish("$uuid$SUCCESS_INDICATOR")

    override fun publishFailure(uuid: String) = publish("$uuid$FAILURE_INDICATOR")

    private fun publish(message: String) {
        log.debug("Publishing $message")
        logTime(log, Level.TRACE, { "Publishing in $it" }) { stringCommands.publish(channelName, message) }
    }

    override fun CoroutineScope.subscribe(uuid: String, timeout: Duration): Deferred<PollStatus> {
        val deferred = CompletableDeferred<PollStatus>()
        // TODO maybe we can re-use a single connection for the subscriptions, too? But we have to be careful regarding the subscribe()/unsubscribe() interferences. Maybe keep being subscribed?
        val subscribeConnection = redisClient.connectPubSub()
        val subscription = subscribeConnection.reactive()
        subscription.subscribe(channelName).subscribe()
        // Deferred can be completed/cancelled in two ways:
        // - Completion on matching message or on timeout
        // - Cancellation from outside (e.g. since the job was already finished)
        deferred.invokeOnCompletion {
            subscription.unsubscribe().subscribe()
            // for some reason we have to delay closing the connection, otherwise we're getting errors like this:
            // ERROR reactor.core.publisher.Operators - Operator called default onErrorDropped
            thread { Thread.sleep(CONNECTION_CLOSE_DELAY); subscribeConnection.close() }
        }
        subscription.observeChannels().doOnNext {
            val message = it.message
            val channel = it.channel
            log.trace("Received message $message on channel $channel")
            if (message?.startsWith(uuid) == true) {
                log.debug("Received matching message $message on channel $channel")
                val status =
                    if (message.endsWith(SUCCESS_INDICATOR))
                        PollStatus.SUCCESS
                    else if (message.endsWith(FAILURE_INDICATOR))
                        PollStatus.FAILURE
                    else
                        error("Unexpected Redis Pub/Sub message: $message")
                deferred.complete(status)
            }
        }.timeout(timeout.toJavaDuration()) {
            deferred.complete(PollStatus.TIMEOUT)
        }.subscribe()
        return deferred
    }
}
