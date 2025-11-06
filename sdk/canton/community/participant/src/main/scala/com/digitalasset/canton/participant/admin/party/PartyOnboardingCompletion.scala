// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.sync.ConnectedSynchronizer
import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

final class PartyOnboardingCompletion(
    partyId: PartyId,
    targetParticipantId: ParticipantId,
    connectedSynchronizer: ConnectedSynchronizer,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  private val topologyWorkflow =
    new PartyReplicationTopologyWorkflow(
      targetParticipantId,
      timeouts = ProcessingTimeout(),
      loggerFactory,
    )

  def attemptCompletion(
      onboardingEffectiveAt: CantonTimestamp
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, (Boolean, Option[CantonTimestamp])] =
    topologyWorkflow.authorizeOnboardedTopology(
      partyId,
      targetParticipantId,
      onboardingEffectiveAt,
      connectedSynchronizer,
    )

}
