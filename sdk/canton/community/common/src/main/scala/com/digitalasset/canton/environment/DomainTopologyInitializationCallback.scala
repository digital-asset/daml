// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import cats.data.EitherT
import com.digitalasset.canton.sequencing.client.transports.SequencerClientTransport
import com.digitalasset.canton.sequencing.protocol.TopologyStateForInitRequest
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX.GenericStoredTopologyTransactionsX
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.TopologyStoreX
import com.digitalasset.canton.topology.transaction.{DomainTrustCertificateX, MediatorDomainStateX}
import com.digitalasset.canton.topology.{MediatorId, Member, ParticipantId}
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

      timestampsFromOnboardingTransactions <- member match {
        case participantId @ ParticipantId(_) =>
          val fromOnboardingTransaction = response.topologyTransactions.value.result
            .dropWhile(storedTx =>
              !storedTx
                .selectMapping[DomainTrustCertificateX]
                .exists(dtc =>
                  dtc.mapping.participantId == participantId && !dtc.transaction.isProposal
                )
            )
            .map(storedTx => storedTx.sequenced -> storedTx.validFrom)
          EitherT.fromEither[Future](
            Either.cond(
              fromOnboardingTransaction.nonEmpty,
              fromOnboardingTransaction,
              s"no domain trust certificate by $participantId found",
            )
          )
        case mediatorId @ MediatorId(_) =>
          val fromOnboardingTransaction = response.topologyTransactions.value.result
            .dropWhile(storedTx =>
              !storedTx
                .selectMapping[MediatorDomainStateX]
                .exists(mds =>
                  mds.mapping.allMediatorsInGroup
                    .contains(mediatorId) && !mds.transaction.isProposal
                )
            )
            .map(storedTx => storedTx.sequenced -> storedTx.validFrom)

          EitherT.fromEither[Future](
            Either.cond(
              fromOnboardingTransaction.nonEmpty,
              fromOnboardingTransaction,
              s"no mediator state including $mediatorId found",
            )
          )
        case unexpectedMemberType =>
          EitherT.leftT[Future, Seq[(SequencedTime, EffectiveTime)]](
            s"unexpected member type: $unexpectedMemberType"
          )
      }

      // Update the topology client head not only with the onboarding transaction, but all subsequent topology
      // transactions as well. This ensures that the topology client can serve topology snapshots at timestamps between
      // post-onboarding topology changes.
      _ = timestampsFromOnboardingTransactions
        .foreach { case (sequenced, effective) =>
          updateTopologyClientHead(topologyClient, sequenced, effective)
        }
    } yield {
      response.topologyTransactions.value
    }
  }

  private def updateTopologyClientHead(
      topologyClient: DomainTopologyClientWithInit,
      st: SequencedTime,
      et: EffectiveTime,
  )(implicit traceContext: TraceContext): Unit = {
    topologyClient.updateHead(
      et,
      st.toApproximate,
      potentialTopologyChange = true,
    )
    if (et.value != st.value) {
      topologyClient.updateHead(
        et,
        et.toApproximate,
        potentialTopologyChange = true,
      )
    }
  }
}
