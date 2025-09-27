// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.SynchronizerTimeTracker
import com.digitalasset.canton.topology.client.SynchronizerTopologyClient
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.{
  ParticipantId,
  PartyId,
  SynchronizerId,
  SynchronizerTopologyManager,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

final class PartyOnboardingCompletion(
    partyId: PartyId,
    synchronizerId: SynchronizerId,
    targetParticipantId: ParticipantId,
    synchronizerTimeTracker: SynchronizerTimeTracker,
    topologyManager: SynchronizerTopologyManager,
    topologyStore: TopologyStore[SynchronizerStore],
    topologyClient: SynchronizerTopologyClient,
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
      synchronizerId,
      targetParticipantId,
      onboardingEffectiveAt,
      synchronizerTimeTracker,
      topologyManager,
      topologyStore,
      topologyClient,
    )

}
