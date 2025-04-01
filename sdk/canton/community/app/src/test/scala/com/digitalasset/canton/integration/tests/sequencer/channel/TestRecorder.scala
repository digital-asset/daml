// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer.channel

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.crypto.{Encrypted, SymmetricKey, SynchronizerCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.{ProtocolSymmetricKey, StaticSynchronizerParameters}
import com.digitalasset.canton.sequencing.channel.ConnectToSequencerChannelRequest
import com.digitalasset.canton.sequencing.protocol.channel.{
  SequencerChannelSessionKey,
  SequencerChannelSessionKeyAck,
}
import com.digitalasset.canton.serialization.DefaultDeserializationError
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.mutable
import scala.concurrent.ExecutionContext

/** Records the sequencer channel requests messages sent from a sequencer channel client endpoint to
  * the sequencer channel service.
  */
private[channel] final class TestRecorder(
    participant: LocalParticipantReference,
    synchronizerId: SynchronizerId,
    staticSynchronizerParameters: StaticSynchronizerParameters,
)(implicit executionContext: ExecutionContext, traceContext: TraceContext) {

  private lazy val messages: mutable.Buffer[ConnectToSequencerChannelRequest] = mutable.Buffer.empty

  private[channel] def recordSentMessage(message: ConnectToSequencerChannelRequest): Unit =
    messages += message

  private[channel] def fetchSessionKeyMessages: Seq[SequencerChannelSessionKey] =
    messages
      .filter(req =>
        req.request match {
          case ConnectToSequencerChannelRequest.SessionKey(_) => true
          case _ => false
        }
      )
      .collect(req =>
        req.request match {
          case ConnectToSequencerChannelRequest.SessionKey(sessionKey) => Some(sessionKey)
          case _ => None
        }
      )
      .flatten
      .toSeq

  private[channel] def fetchSessionKeyAcks: Seq[SequencerChannelSessionKeyAck] =
    messages
      .filter(req =>
        req.request match {
          case ConnectToSequencerChannelRequest.SessionKeyAck(_) => true
          case _ => false
        }
      )
      .collect(req =>
        req.request match {
          case ConnectToSequencerChannelRequest.SessionKeyAck(sessionKeyAck) => Some(sessionKeyAck)
          case _ => None
        }
      )
      .flatten
      .toSeq

  private[channel] def extractSessionKey(
      sessionKeyMessage: SequencerChannelSessionKey
  ): EitherT[FutureUnlessShutdown, String, SymmetricKey] =
    for {
      recentSnapshot <- EitherT.right(crypto.snapshot(timestamp))
      key <- recentSnapshot
        .decrypt(sessionKeyMessage.encryptedSessionKey)(bytes =>
          ProtocolSymmetricKey
            .fromTrustedByteString(bytes)
            .leftMap(error => DefaultDeserializationError(error.message))
        )
        .leftMap(_.toString)
    } yield key.unwrap

  private[channel] def convertPayloadMessagesToString(key: SymmetricKey): Seq[String] =
    messages
      .filter(req =>
        req.request match {
          case ConnectToSequencerChannelRequest.Payload(_) => true
          case _ => false
        }
      )
      .collect(req =>
        req.request match {
          case ConnectToSequencerChannelRequest.Payload(payload) =>
            val encryptedPayload = Encrypted.fromByteString(payload)
            val decrypted = crypto.pureCrypto.decryptWith(encryptedPayload, key)(Right(_))
            val res = decrypted match {
              case Right(byteString) => byteString.toString("UTF-8")
              case Left(error) => error.toString
            }
            res
          case _ => "these aren't the payloads you're looking for"
        }
      )
      .toSeq

  private[channel] def crypto: SynchronizerCryptoClient =
    participant.testing
      .crypto_api()
      .forSynchronizer(synchronizerId, staticSynchronizerParameters)
      .getOrElse(throw new RuntimeException("crypto api for synchronizer is unavailable"))

  private[channel] def timestamp: CantonTimestamp =
    participant.topology.transactions
      .list(store = TopologyStoreId.Synchronizer(synchronizerId))
      .result
      .last
      .validFrom
      .value

}
