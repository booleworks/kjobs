// SPDX-License-Identifier: MIT
// Copyright 2023 BooleWorks GmbH

package com.booleworks.kjobs.data

import com.booleworks.kjobs.control.Maintenance
import java.time.LocalDateTime

/**
 * Representation of a heartbeat. Heartbeats are used to determine if an instance is still alive.
 * Each instance regularly updates its heartbeat in the DB using
 * [the respective maintenance job][Maintenance.updateHeartbeat].
 */
data class Heartbeat(val instanceName: String, val lastBeat: LocalDateTime)
