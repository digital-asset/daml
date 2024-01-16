// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.SyncDomainEphemeralState
import com.digitalasset.canton.protocol.messages.{LocalReject, MediatorResponse}
import com.digitalasset.canton.protocol.{RequestId, RootHash}
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.sequencing.protocol.Recipients
import com.digitalasset.canton.topology.{DomainId, MediatorRef, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPartyId, SequencerCounter, checked}

import scala.concurrent.ExecutionContext

class BadRootHashMessagesRequestProcessor(
    ephemeral: SyncDomainEphemeralState,
    crypto: DomainSyncCryptoClient,
    sequencerClient: SequencerClient,
    domainId: DomainId,
    participantId: ParticipantId,
    protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends AbstractMessageProcessor(
      ephemeral,
      crypto,
      sequencerClient,
      protocolVersion,
    ) {

  /** Sends a [[com.digitalasset.canton.protocol.messages.Malformed]]
    * for the given [[com.digitalasset.canton.protocol.RootHash]] with the given `rejectionReason`.
    * Also ticks the record order publisher.
    */
  def sendRejectionAndTerminate(
      sequencerCounter: SequencerCounter,
      timestamp: CantonTimestamp,
      rootHash: RootHash,
      mediator: MediatorRef,
      reject: LocalReject,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    performUnlessClosingUSF(functionFullName) {
      for {
        snapshot <- crypto.snapshotUS(timestamp)
        requestId = RequestId(timestamp)
        rejection = checked(
          MediatorResponse.tryCreate(
            requestId = requestId,
            sender = participantId,
            viewPositionO = None,
            localVerdict = reject,
            rootHash = Some(rootHash),
            confirmingParties = Set.empty,
            domainId = domainId,
            protocolVersion = protocolVersion,
          )
        )
        signedRejection <- FutureUnlessShutdown.outcomeF(signResponse(snapshot, rejection))
        _ <- sendResponses(
          requestId,
          Seq(signedRejection -> Recipients.cc(mediator.toRecipient)),
        ).mapK(FutureUnlessShutdown.outcomeK)
          .valueOr(
            // This is a best-effort response anyway, so we merely log the failure and continue
            error =>
              logger.warn(show"Failed to send best-effort rejection of malformed request: $error")
          )
        _ = ephemeral.recordOrderPublisher.tick(sequencerCounter, timestamp)
      } yield ()
    }

  def participantIsAddressByPartyGroupAddress(
      timestamp: CantonTimestamp,
      parties: Seq[LfPartyId],
      participantId: ParticipantId,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    performUnlessClosingUSF(functionFullName) {
      for {
        snapshot <- crypto.awaitIpsSnapshotUS(timestamp)
        p <- FutureUnlessShutdown.outcomeF(snapshot.activeParticipantsOfParties(parties))
      } yield p.values.exists(_.contains(participantId))
    }
}
