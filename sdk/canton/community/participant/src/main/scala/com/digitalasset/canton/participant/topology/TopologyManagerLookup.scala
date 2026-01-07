// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.data.EitherT
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.topology.admin.grpc.PSIdLookup
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.{
  PhysicalSynchronizerId,
  SynchronizerId,
  SynchronizerTopologyManager,
  TopologyManagerError,
}

import scala.concurrent.ExecutionContext

class TopologyManagerLookup(
    lookupByPsid: PhysicalSynchronizerId => Option[SynchronizerTopologyManager],
    lookupActivePsidByLsid: PSIdLookup,
) {

  def byPhysicalSynchronizerId(psid: PhysicalSynchronizerId)(implicit
      errorLoggingContext: ErrorLoggingContext,
      ec: ExecutionContext,
  ): EitherT[
    FutureUnlessShutdown,
    ParticipantTopologyManagerError,
    SynchronizerTopologyManager,
  ] =
    EitherT.fromOption[FutureUnlessShutdown](
      lookupByPsid(psid),
      ParticipantTopologyManagerError.IdentityManagerParentError(
        TopologyManagerError.TopologyStoreUnknown.Failure(SynchronizerStore(psid))
      ),
    )

  def activeBySynchronizerId(lsid: SynchronizerId)(implicit
      errorLoggingContext: ErrorLoggingContext,
      ec: ExecutionContext,
  ): EitherT[
    FutureUnlessShutdown,
    ParticipantTopologyManagerError,
    SynchronizerTopologyManager,
  ] =
    EitherT.fromOption[FutureUnlessShutdown](
      lookupActivePsidByLsid.activePSIdFor(lsid).flatMap(lookupByPsid),
      ParticipantTopologyManagerError.IdentityManagerParentError(
        TopologyManagerError.TopologyStoreUnknown.NotFoundForSynchronizer(lsid)
      ),
    )

}
