// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.channel.endpoint

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.EncryptionError.FailedToEncrypt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.ProtocolSymmetricKey
import com.digitalasset.canton.serialization.{DefaultDeserializationError, DeserializationError}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.SingleUseCell
import com.digitalasset.canton.version.{HasToByteString, ProtocolVersion}
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

/** Encapsulates the sequencer channel security concern, in particular in stores the session key in-memory.
  *
  * Instances of this class are expected to be used by
  * [[com.digitalasset.canton.sequencing.client.channel.endpoint.SequencerChannelClientEndpoint]], and destroyed when
  * such endpoint is no longer needed.
  *
  * Using the [[com.digitalasset.canton.crypto.SynchronizerCryptoClient]] as opposed to using
  * [[com.digitalasset.canton.crypto.SyncCryptoClient]] (`SyncCryptoClient[SyncCryptoApi]`) ensures that the crypto
  * schemes that the synchronizer prescribes are actually used. Doing so prevents downgrading attacks where a weaker scheme
  * is used.
  *
  * The [[com.digitalasset.canton.data.CantonTimestamp]] deterministically determines the public key that a member
  * uses to encrypt / decrypt the symmetric session key. That timestamp is expected to originate from a recent topology
  * transaction that has already taken effect; meaning it's not a future timestamp.
  *
  * @param synchronizerCryptoApi Provides the crypto API for symmetric and asymmetric encryption operations.
  * @param protocolVersion Used for the proto messages versioning.
  * @param timestamp   Determines the public key for asymmetric encryption.
  */
private[endpoint] final class SequencerChannelSecurity(
    synchronizerCryptoApi: SynchronizerCryptoClient,
    protocolVersion: ProtocolVersion,
    timestamp: CantonTimestamp,
)(implicit executionContext: ExecutionContext) {

  private val pureCrypto = synchronizerCryptoApi.pureCrypto

  private val sessionKey = new SingleUseCell[SymmetricKey]

  private def registerSessionKey(key: SymmetricKey): Unit =
    sessionKey.putIfAbsent(key).discard

  private[endpoint] def generateSessionKey(connectTo: Member)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, AsymmetricEncrypted[ProtocolSymmetricKey]] =
    for {
      key <- EitherT
        .fromEither[FutureUnlessShutdown](pureCrypto.generateSymmetricKey())
        .leftMap(_.toString)
      encryptedKey <- encrypt(connectTo, key)(traceContext)
    } yield {
      registerSessionKey(key)
      encryptedKey
    }

  private[endpoint] def registerSessionKey(
      encrypted: AsymmetricEncrypted[ProtocolSymmetricKey]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      recentSnapshot <- EitherT.right(synchronizerCryptoApi.snapshot(timestamp))
      key <- recentSnapshot
        .decrypt(encrypted)(bytes =>
          ProtocolSymmetricKey
            .fromByteString(protocolVersion, bytes)
            .leftMap(error => DefaultDeserializationError(error.message))
        )
        .leftMap(_.toString)
    } yield registerSessionKey(key.unwrap)

  private def encrypt(connectTo: Member, key: SymmetricKey)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, AsymmetricEncrypted[ProtocolSymmetricKey]] =
    for {
      recentSnapshot <- EitherT.right(synchronizerCryptoApi.snapshot(timestamp))
      // asymmetrically encrypted for the connectTo member
      encryptedPerMember <- recentSnapshot
        .encryptFor(
          ProtocolSymmetricKey(key, protocolVersion),
          Seq(connectTo),
        )
        .leftMap(_.toString)
      encrypted <- EitherT.fromOption[FutureUnlessShutdown](
        encryptedPerMember.get(connectTo),
        s"No encrypted symmetric key found for $connectTo",
      )
    } yield encrypted

  private[endpoint] def encrypt[M <: HasToByteString](
      message: M
  ): EitherT[FutureUnlessShutdown, EncryptionError, Encrypted[M]] =
    for {
      key <- EitherT
        .fromOption[FutureUnlessShutdown](sessionKey.get, FailedToEncrypt("No session key found"))
      encryptedMessage <- EitherT.fromEither[FutureUnlessShutdown](
        pureCrypto.encryptSymmetricWith(message, key)
      )
    } yield encryptedMessage

  private[endpoint] def decrypt[M](
      encryptedMessage: Encrypted[M]
  )(
      deserialize: ByteString => Either[DeserializationError, M]
  ): EitherT[Future, String, M] =
    for {
      key <- EitherT.fromOption[Future](sessionKey.get, "No session key found")
      message <- EitherT.fromEither[Future](
        pureCrypto.decryptWith(encryptedMessage, key)(deserialize).leftMap(_.toString)
      )
    } yield message

}
