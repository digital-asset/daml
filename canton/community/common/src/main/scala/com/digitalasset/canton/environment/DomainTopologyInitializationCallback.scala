// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import cats.data.EitherT
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.client.transports.SequencerClientTransport
import com.digitalasset.canton.sequencing.protocol.TopologyStateForInitRequest
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX.GenericStoredTopologyTransactionsX
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.TopologyStoreX
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

trait DomainTopologyInitializationCallback {
  def callback(
      topologyClient: DomainTopologyClientWithInit,
      clientTransport: SequencerClientTransport,
      protocolVersion: ProtocolVersion,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, GenericStoredTopologyTransactionsX]
}

class StoreBasedDomainTopologyInitializationCallback(
    member: Member,
    topologyStore: TopologyStoreX[DomainStore],
) extends DomainTopologyInitializationCallback {
  override def callback(
      topologyClient: DomainTopologyClientWithInit,
      transport: SequencerClientTransport,
      protocolVersion: ProtocolVersion,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, GenericStoredTopologyTransactionsX] = {
    for {
      response <- transport.downloadTopologyStateForInit(
        TopologyStateForInitRequest(
          member,
          protocolVersion,
        )
      )
      _ <- EitherT.liftF(
        topologyStore.bootstrap(response.topologyTransactions.value)
      )
      _ <- EitherT.liftF(
        response.topologyTransactions.value.lastChangeTimestamp
          .map(updateTopologyClientHead(topologyClient, protocolVersion, _))
          .getOrElse(Future.unit)
      )
    } yield {
      response.topologyTransactions.value
    }
  }

  private def updateTopologyClientHead(
      topologyClient: DomainTopologyClientWithInit,
      protocolVersion: ProtocolVersion,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext, ec: ExecutionContext): Future[Unit] = {
    // first update the head with the timestamp immediately after the last topology change update.
    // this let's us find the transaction with the dynamic domain parameters (specifically, the topology change delay)
    val tsNext = EffectiveTime(timestamp.immediateSuccessor)
    topologyClient.updateHead(
      tsNext,
      tsNext.toApproximate,
      potentialTopologyChange = true,
    )

    // fetch the topology change delay from the dynamic domain parameters
    topologyClient
      .awaitSnapshot(tsNext.value)
      .flatMap(
        _.findDynamicDomainParametersOrDefault(
          protocolVersion,
          warnOnUsingDefault = false,
        )
      )
      .map { params =>
        // update the client with the proper topology change delay
        val withTopoChangeDelay = EffectiveTime(timestamp.plus(params.topologyChangeDelay.duration))
        topologyClient.updateHead(
          withTopoChangeDelay,
          withTopoChangeDelay.toApproximate,
          potentialTopologyChange = true,
        )
      }
  }
}
