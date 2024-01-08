// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.Eval
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.sync.ParticipantEventPublisher
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration}
import com.digitalasset.canton.topology.ParticipantId

import scala.concurrent.ExecutionContext

/** Some of the state of a participant that is not tied to a domain and is kept only in memory.
  */
class ParticipantNodeEphemeralState(
    val participantEventPublisher: ParticipantEventPublisher
)

object ParticipantNodeEphemeralState {
  def apply(
      participantId: ParticipantId,
      persistentState: Eval[ParticipantNodePersistentState],
      clock: Clock,
      maxDeduplicationDuration: Eval[NonNegativeFiniteDuration],
      timeouts: ProcessingTimeout,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): ParticipantNodeEphemeralState = {
    val participantEventPublisher = new ParticipantEventPublisher(
      participantId,
      persistentState.map(_.participantEventLog),
      persistentState.map(_.multiDomainEventLog),
      clock,
      maxDeduplicationDuration.map(_.unwrap),
      timeouts,
      futureSupervisor,
      loggerFactory,
    )
    new ParticipantNodeEphemeralState(
      participantEventPublisher
    )
  }
}
