// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.sync.SyncPersistentStateManager
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.client.StoreBasedSynchronizerTopologyClient
import com.digitalasset.canton.topology.processing.{ApproximateTime, EffectiveTime, SequencedTime}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

class SortedReconciliationIntervalsProviderFactory(
    syncPersistentStateManager: SyncPersistentStateManager,
    futureSupervisor: FutureSupervisor,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {
  def get(synchronizerId: SynchronizerId, subscriptionTs: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, SortedReconciliationIntervalsProvider] =
    for {
      syncPersistentState <- EitherT.fromEither[FutureUnlessShutdown](
        syncPersistentStateManager
          .get(synchronizerId)
          .toRight(
            s"Unable to get synchronizer persistent state for synchronizer $synchronizerId"
          )
      )

      staticSynchronizerParameters <- EitherT(
        syncPersistentState.parameterStore.lastParameters.map(
          _.toRight(
            s"Unable to fetch static synchronizer parameters for synchronizer $synchronizerId"
          )
        )
      )
      topologyFactory <- syncPersistentStateManager
        .topologyFactoryFor(synchronizerId, staticSynchronizerParameters.protocolVersion)
        .toRight(s"Can not obtain topology factory for $synchronizerId")
        .toEitherT[FutureUnlessShutdown]
    } yield {
      val topologyClient = topologyFactory.createTopologyClient(
        StoreBasedSynchronizerTopologyClient.NoPackageDependencies
      )
      topologyClient.updateHead(
        SequencedTime(subscriptionTs),
        EffectiveTime(subscriptionTs),
        ApproximateTime(subscriptionTs),
        potentialTopologyChange = true,
      )

      new SortedReconciliationIntervalsProvider(
        topologyClient,
        futureSupervisor,
        loggerFactory,
      )
    }

}
