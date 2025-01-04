// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.checked
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.SyncDomainEphemeralState
import com.digitalasset.canton.protocol.messages.ConfirmationResponse
import com.digitalasset.canton.protocol.{LocalRejectError, RequestId, RootHash}
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.sequencing.protocol.{MediatorGroupRecipient, Recipients}
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

class BadRootHashMessagesRequestProcessor(
    ephemeral: SyncDomainEphemeralState,
    crypto: DomainSyncCryptoClient,
    sequencerClient: SequencerClient,
    synchronizerId: SynchronizerId,
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
      synchronizerId,
    ) {

  /** Sends `reject` for the given `rootHash`.
    * Also ticks the record order publisher.
    */
  def sendRejectionAndTerminate(
      timestamp: CantonTimestamp,
      rootHash: RootHash,
      mediator: MediatorGroupRecipient,
      reject: LocalRejectError,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    performUnlessClosingUSF(functionFullName) {
      for {
        snapshot <- crypto.awaitSnapshotUS(timestamp)
        requestId = RequestId(timestamp)
        _ = reject.log()
        rejection = checked(
          ConfirmationResponse.tryCreate(
            requestId = requestId,
            sender = participantId,
            viewPositionO = None,
            localVerdict = reject.toLocalReject(protocolVersion),
            rootHash = rootHash,
            confirmingParties = Set.empty,
            synchronizerId = synchronizerId,
            protocolVersion = protocolVersion,
          )
        )
        signedRejection <- signResponse(snapshot, rejection)
        _ <- sendResponses(
          requestId,
          Seq(signedRejection -> Recipients.cc(mediator)),
        )
      } yield ()
    }
}
