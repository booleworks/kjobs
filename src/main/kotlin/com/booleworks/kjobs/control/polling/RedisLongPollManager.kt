// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.control.polling

import com.booleworks.kjobs.data.PollStatus
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runInterruptible
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPubSub
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Duration

private val log: Logger = LoggerFactory.getLogger("RedisLongPollManager")

private const val DEFAULT_REDIS_CHANNEL = "job_finished"

private const val SUCCESS_INDICATOR = "||SUCCESS"
private const val FAILURE_INDICATOR = "||FAILURE"

private const val SUBSCRIBE_RETRIES = 3

class RedisLongPollManager(private val pool: JedisPool, private val channelName: String = DEFAULT_REDIS_CHANNEL) : LongPollManager {

    override fun publishSuccess(uuid: String) = publish("$uuid$SUCCESS_INDICATOR")

    override fun publishFailure(uuid: String) = publish("$uuid$FAILURE_INDICATOR")

    private fun publish(message: String) {
        log.debug("Publishing $message")
        pool.resource.publish(channelName, message)
    }

    override fun CoroutineScope.subscribe(uuid: String, timeout: Duration): Deferred<PollStatus> {
        val deferred = CompletableDeferred<PollStatus>()
        val subscription = KJobsPubSub(uuid, deferred)
        return pool.resource.use { jedis ->
            val subscribeJob = launch(Dispatchers.IO + CoroutineName("Long poll subscription for job $uuid")) {
                var i = 1
                while (true) {
                    runCatching {
                        runInterruptible(Dispatchers.IO) { jedis.subscribe(subscription, channelName) }
                    }.onFailure {
                        if (it is CancellationException || it.cause is InterruptedException) {
                            log.info("Subscription for ID $uuid was interrupted")
                            return@launch
                        }
                        if (i++ < SUBSCRIBE_RETRIES) {
                            log.warn("Subscribing for UUID $uuid failed -- retrying", it)
                        } else {
                            log.error("Subscribing for UUID $uuid failed -- aborting")
                            return@launch
                        }
                    }
                }
            }
            val unsubscribeHandler = launch(Dispatchers.IO + CoroutineName("Long poll unsubscribe handler for job $uuid with timeout $timeout")) {
                delay(timeout)
                log.trace("Subscription waiting for job $uuid timed out after $timeout")
                deferred.complete(PollStatus.TIMEOUT)
            }
            deferred.invokeOnCompletion {
                subscription.unsubscribe()
                subscribeJob.cancel()
                unsubscribeHandler.cancel()
            }
            deferred
        }
    }
}

private class KJobsPubSub(val uuid: String, val deferred: CompletableDeferred<PollStatus>) : JedisPubSub() {
    override fun onMessage(channel: String?, message: String?) {
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
    }
}
