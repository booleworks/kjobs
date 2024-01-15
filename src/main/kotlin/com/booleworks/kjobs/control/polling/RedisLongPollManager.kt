// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.control.polling

import com.booleworks.kjobs.data.PollStatus
import io.lettuce.core.RedisClient
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kotlin.time.Duration
import kotlin.time.toJavaDuration

private val log: Logger = LoggerFactory.getLogger("RedisLongPollManager")

private const val DEFAULT_REDIS_CHANNEL = "job_finished"

private const val SUCCESS_INDICATOR = "||SUCCESS"
private const val FAILURE_INDICATOR = "||FAILURE"

class RedisLongPollManager(protected val redisClient: RedisClient, protected val channelName: String = DEFAULT_REDIS_CHANNEL) : LongPollManager {
    val syncCommands = redisClient.connect().sync()

    override fun publishSuccess(uuid: String) = publish("$uuid$SUCCESS_INDICATOR")

    override fun publishFailure(uuid: String) = publish("$uuid$FAILURE_INDICATOR")

    private fun publish(message: String) {
        log.debug("Publishing $message")
        syncCommands.publish(channelName, message)
    }

    override fun CoroutineScope.subscribe(uuid: String, timeout: Duration): Deferred<PollStatus> {
        val deferred = CompletableDeferred<PollStatus>()
        val subscription = redisClient.connectPubSub().reactive()
        subscription.subscribe(channelName).subscribe()
        // Deferred can be completed/cancelled in two ways:
        // - Completion on matching message or on timeout
        // - Cancellation from outside (e.g. since the job was already finished)
        deferred.invokeOnCompletion {
            subscription.unsubscribe().subscribe()
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
