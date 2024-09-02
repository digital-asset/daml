// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.Eval
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker
import com.digitalasset.canton.participant.sync.ParticipantEventPublisher
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.ParticipantId

import scala.concurrent.ExecutionContext

/** Some of the state of a participant that is not tied to a domain and is kept only in memory.
  */
class ParticipantNodeEphemeralState(
    val participantEventPublisher: ParticipantEventPublisher,
    val inFlightSubmissionTracker: InFlightSubmissionTracker,
)

object ParticipantNodeEphemeralState {
  def apply(
      participantId: ParticipantId,
      ledgerApiIndexer: Eval[LedgerApiIndexer],
      inFlightSubmissionTracker: InFlightSubmissionTracker,
      clock: Clock,
      exitOnFatalFailures: Boolean,
      timeouts: ProcessingTimeout,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): ParticipantNodeEphemeralState = {
    val participantEventPublisher = new ParticipantEventPublisher(
      participantId,
      ledgerApiIndexer,
      clock,
      exitOnFatalFailures = exitOnFatalFailures,
      timeouts,
      futureSupervisor,
      loggerFactory,
    )
    new ParticipantNodeEphemeralState(
      participantEventPublisher,
      inFlightSubmissionTracker,
    )
  }
}
