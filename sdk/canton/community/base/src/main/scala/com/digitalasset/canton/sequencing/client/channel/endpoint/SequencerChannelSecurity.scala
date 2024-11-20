// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.channel.endpoint

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.EncryptionError.FailedToEncrypt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
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
  * Using the [[com.digitalasset.canton.crypto.DomainSyncCryptoClient]] as opposed to using
  * [[com.digitalasset.canton.crypto.SyncCryptoClient]] (`SyncCryptoClient[SyncCryptoApi]`) ensures that the crypto
  * schemes that the domain prescribes are actually used. Doing so prevents downgrading attacks where a weaker scheme
  * is used.
  *
  * The [[com.digitalasset.canton.data.CantonTimestamp]] deterministically determines the public key that a member
  * uses to encrypt / decrypt the symmetric session key. That timestamp is expected to originate from a recent topology
  * transaction that has already taken effect; meaning it's not a future timestamp.
  *
  * @param domainCryptoApi Provides the crypto API for symmetric and asymmetric encryption operations.
  * @param protocolVersion Used for the proto messages versioning.
  * @param timestamp   Determines the public key for asymmetric encryption.
  */
private[endpoint] final class SequencerChannelSecurity(
    domainCryptoApi: DomainSyncCryptoClient,
    protocolVersion: ProtocolVersion,
    timestamp: CantonTimestamp,
)(implicit executionContext: ExecutionContext) {

  private val pureCrypto = domainCryptoApi.pureCrypto

  private val sessionKey = new SingleUseCell[SymmetricKey]

  private def registerSessionKey(key: SymmetricKey): Unit =
    sessionKey.putIfAbsent(key).discard

  private[endpoint] def generateSessionKey(connectTo: Member)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, AsymmetricEncrypted[SymmetricKey]] =
    for {
      key <- EitherT
        .fromEither[Future](pureCrypto.generateSymmetricKey())
        .leftMap(_.toString)
      encryptedKey <- encrypt(connectTo, key)(traceContext)
    } yield {
      registerSessionKey(key)
      encryptedKey
    }

  private[endpoint] def registerSessionKey(encrypted: AsymmetricEncrypted[SymmetricKey])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      recentSnapshot <- EitherT.right(domainCryptoApi.snapshotUS(timestamp))
      key <- recentSnapshot
        .decrypt(encrypted)(bytes =>
          SymmetricKey
            .fromTrustedByteString(bytes) // this is fine because it's inside an encrypted message
            .leftMap(error => DefaultDeserializationError(error.message))
        )
        .leftMap(_.toString)
    } yield registerSessionKey(key)

  private def encrypt(connectTo: Member, key: SymmetricKey)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, AsymmetricEncrypted[SymmetricKey]] =
    for {
      recentSnapshot <- EitherT.right(domainCryptoApi.snapshot(timestamp))
      // asymmetrically encrypted for the connectTo member
      encryptedPerMember <- recentSnapshot
        .encryptFor(
          key,
          Seq(connectTo),
          protocolVersion,
        )
        .leftMap(_.toString)
      encrypted <- EitherT.fromOption[Future](
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
        pureCrypto.encryptWith(message, key)
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
