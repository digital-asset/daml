// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.sync.SyncDomainPersistentStateManager
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.topology.client.StoreBasedDomainTopologyClient
import com.digitalasset.canton.topology.processing.{ApproximateTime, EffectiveTime}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

class SortedReconciliationIntervalsProviderFactory(
    syncDomainPersistentStateManager: SyncDomainPersistentStateManager,
    futureSupervisor: FutureSupervisor,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {
  def get(domainId: DomainId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, SortedReconciliationIntervalsProvider] =
    for {
      syncDomainPersistentState <- EitherT.fromEither[Future](
        syncDomainPersistentStateManager
          .get(domainId)
          .toRight(s"Unable to get sync domain persistent state for domain $domainId")
      )

      staticDomainParameters <- EitherT(
        syncDomainPersistentState.parameterStore.lastParameters.map(
          _.toRight(s"Unable to fetch static domain parameters for domain $domainId")
        )
      )

      subscriptionTs <- EitherT.liftF(
        syncDomainPersistentState.sequencerCounterTrackerStore.preheadSequencerCounter
          .map(_.fold(CantonTimestamp.MinValue)(_.timestamp))
      )
      topologyFactory <- syncDomainPersistentStateManager
        .topologyFactoryFor(domainId)
        .toRight(s"Can not obtain topology factory for ${domainId}")
        .toEitherT[Future]
    } yield {
      val topologyClient = topologyFactory.createTopologyClient(
        staticDomainParameters.protocolVersion,
        StoreBasedDomainTopologyClient.NoPackageDependencies,
      )
      topologyClient.updateHead(
        EffectiveTime(subscriptionTs),
        ApproximateTime(subscriptionTs),
        potentialTopologyChange = true,
      )

      SortedReconciliationIntervalsProvider(
        staticDomainParameters,
        topologyClient,
        futureSupervisor,
        loggerFactory,
      )
    }

}
