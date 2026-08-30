// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import cats.data.EitherT
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.topology.client.SynchronizerTopologyClientWithInit
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  InitialTopologySnapshotValidator,
  SequencedTime,
}
import com.digitalasset.canton.topology.transaction.{
  MediatorSynchronizerState,
  SynchronizerTrustCertificate,
}
import com.digitalasset.canton.topology.{MediatorId, Member, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

trait SynchronizerTopologyInitializationCallback {
  def callback(
      topologyStoreInitialization: InitialTopologySnapshotValidator,
      topologyClient: SynchronizerTopologyClientWithInit,
      sequencerClient: SequencerClient,
      protocolVersion: ProtocolVersion,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Unit]
}

class StoreBasedSynchronizerTopologyInitializationCallback(
    member: Member
) extends SynchronizerTopologyInitializationCallback {
  override def callback(
      topologyStoreInitialization: InitialTopologySnapshotValidator,
      topologyClient: SynchronizerTopologyClientWithInit,
      sequencerClient: SequencerClient,
      protocolVersion: ProtocolVersion,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      topologyTransactions <- sequencerClient
        .downloadTopologyStateForInit(maxRetries = retry.Forever, retryLogLevel = None)

      _ <- topologyStoreInitialization.validateAndApplyInitialTopologySnapshot(topologyTransactions)

      timestampsFromOnboardingTransactions <- member match {
        case participantId @ ParticipantId(_) =>
          val fromOnboardingTransaction = topologyTransactions.result
            .dropWhile(storedTx =>
              !storedTx
                .selectMapping[SynchronizerTrustCertificate]
                .exists(dtc =>
                  dtc.mapping.participantId == participantId && !dtc.transaction.isProposal
                )
            )
            .map(storedTx => storedTx.sequenced -> storedTx.validFrom)
          EitherT.fromEither[FutureUnlessShutdown](
            Either.cond(
              fromOnboardingTransaction.nonEmpty,
              fromOnboardingTransaction,
              s"no synchronizer trust certificate by $participantId found",
            )
          )
        case mediatorId @ MediatorId(_) =>
          val fromOnboardingTransaction = topologyTransactions.result
            .dropWhile(storedTx =>
              !storedTx
                .selectMapping[MediatorSynchronizerState]
                .exists(mds =>
                  mds.mapping.allMediatorsInGroup
                    .contains(mediatorId) && !mds.transaction.isProposal
                )
            )
            .map(storedTx => storedTx.sequenced -> storedTx.validFrom)

          EitherT.fromEither[FutureUnlessShutdown](
            Either.cond(
              fromOnboardingTransaction.nonEmpty,
              fromOnboardingTransaction,
              s"no mediator state including $mediatorId found",
            )
          )
        case unexpectedMemberType =>
          EitherT.leftT[FutureUnlessShutdown, Seq[(SequencedTime, EffectiveTime)]](
            s"unexpected member type: $unexpectedMemberType"
          )
      }

      // Update the topology client head not only with the onboarding transaction, but all subsequent topology
      // transactions as well. This ensures that the topology client can serve topology snapshots at timestamps between
      // post-onboarding topology changes.
      _ = timestampsFromOnboardingTransactions.distinct
        .foreach { case (sequenced, effective) =>
          updateTopologyClientHead(topologyClient, sequenced, effective)
        }
    } yield ()

  private def updateTopologyClientHead(
      topologyClient: SynchronizerTopologyClientWithInit,
      st: SequencedTime,
      et: EffectiveTime,
  )(implicit traceContext: TraceContext): Unit = {
    topologyClient.updateHead(
      st,
      et,
      st.toApproximate,
    )
    if (et.value != st.value) {
      topologyClient.updateHead(
        st,
        et,
        et.toApproximate,
      )
    }
  }
}
