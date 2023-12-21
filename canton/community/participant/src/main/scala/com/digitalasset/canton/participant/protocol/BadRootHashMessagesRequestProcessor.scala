// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.{LfPartyId, RequestCounter, SequencerCounter, checked}

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

  /** Immediately moves the request to Confirmed and
    * register a timeout handler at the decision time with the request tracker
    * to cover the case that the mediator does not send a mediator result.
    */
  def handleBadRequestWithExpectedMalformedMediatorRequest(
      requestCounter: RequestCounter,
      sequencerCounter: SequencerCounter,
      timestamp: CantonTimestamp,
      mediator: MediatorRef,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    performUnlessClosingUSF(functionFullName) {
      crypto
        .awaitIpsSnapshotUS(timestamp)
        .flatMap(snapshot => FutureUnlessShutdown.outcomeF(snapshot.isMediatorActive(mediator)))
        .flatMap {
          case true =>
            prepareForMediatorResultOfBadRequest(requestCounter, sequencerCounter, timestamp)
          case false =>
            // If the mediator is not active, then it will not send a result,
            // so we can finish off the request immediately
            invalidRequest(requestCounter, sequencerCounter, timestamp)
        }
    }

  /** Immediately moves the request to Confirmed and registers a timeout handler at the decision time with the request tracker.
    * Also sends a [[com.digitalasset.canton.protocol.messages.Malformed]]
    * for the given [[com.digitalasset.canton.protocol.RootHash]] with the given `rejectionReason`.
    */
  def sendRejectionAndExpectMediatorResult(
      requestCounter: RequestCounter,
      sequencerCounter: SequencerCounter,
      timestamp: CantonTimestamp,
      rootHash: RootHash,
      mediator: MediatorRef,
      reject: LocalReject,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    performUnlessClosingUSF(functionFullName) {
      for {
        _ <- prepareForMediatorResultOfBadRequest(requestCounter, sequencerCounter, timestamp)
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
          requestCounter,
          Seq(signedRejection -> Recipients.cc(mediator.toRecipient)),
        ).mapK(FutureUnlessShutdown.outcomeK)
          .valueOr(
            // This is a best-effort response anyway, so we merely log the failure and continue
            error =>
              logger.warn(show"Failed to send best-effort rejection of malformed request: $error")
          )
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
