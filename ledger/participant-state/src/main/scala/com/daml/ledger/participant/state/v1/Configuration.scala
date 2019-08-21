// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

/** Ledger configuration describing the ledger's time model.
  * Emitted in [[com.daml.ledger.participant.state.v1.Update.ConfigurationChanged]].
  */
final case class Configuration(
    /* The configuration generation. Monotonically increasing. */
    generation: Long,
    /** The time model of the ledger. Specifying the time-to-live bounds for Ledger API commands. */
    timeModel: TimeModel,
    /** The identity of the participant allowed to change the configuration. If not set, any participant
      * can change the configuration. */
    authorizedParticipantId: Option[ParticipantId],
    /** Flag to enable "open world" mode in which submissions from unallocated parties are allowed through. Useful in testing. */
    openWorld: Boolean
)
