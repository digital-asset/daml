// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import cats.data.EitherT
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.transaction.{DomainTrustCertificate, MediatorDomainState}
import com.digitalasset.canton.topology.{MediatorId, Member, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

trait DomainTopologyInitializationCallback {
  def callback(
      topologyClient: DomainTopologyClientWithInit,
      sequencerClient: SequencerClient,
      protocolVersion: ProtocolVersion,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, GenericStoredTopologyTransactions]
}

class StoreBasedDomainTopologyInitializationCallback(
    member: Member,
    topologyStore: TopologyStore[DomainStore],
) extends DomainTopologyInitializationCallback {
  override def callback(
      topologyClient: DomainTopologyClientWithInit,
      sequencerClient: SequencerClient,
      protocolVersion: ProtocolVersion,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, GenericStoredTopologyTransactions] = {
    for {
      topologyTransactions <- sequencerClient.downloadTopologyStateForInit()

      _ <- EitherT.right(topologyStore.bootstrap(topologyTransactions))

      timestampsFromOnboardingTransactions <- member match {
        case participantId @ ParticipantId(_) =>
          val fromOnboardingTransaction = topologyTransactions.result
            .dropWhile(storedTx =>
              !storedTx
                .selectMapping[DomainTrustCertificate]
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
          val fromOnboardingTransaction = topologyTransactions.result
            .dropWhile(storedTx =>
              !storedTx
                .selectMapping[MediatorDomainState]
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
      topologyTransactions
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
