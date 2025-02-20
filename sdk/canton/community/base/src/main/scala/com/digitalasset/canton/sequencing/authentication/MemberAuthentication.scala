// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.authentication

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication.AuthenticationError
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

trait MemberAuthentication {

  def hashSynchronizerNonce(
      nonce: Nonce,
      synchronizerId: SynchronizerId,
      pureCrypto: CryptoPureApi,
  ): Hash

  /** Member concatenates the nonce with the synchronizer's id and signs it (step 3)
    */
  def signSynchronizerNonce(
      member: Member,
      nonce: Nonce,
      synchronizerId: SynchronizerId,
      possibleSigningKeys: NonEmpty[Seq[Fingerprint]],
      crypto: Crypto,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, AuthenticationError, Signature]

}

object MemberAuthentication extends MemberAuthentication {

  import com.digitalasset.canton.util.ShowUtil.*

  def apply(member: Member): Either[AuthenticationError, MemberAuthentication] = member match {
    case _: ParticipantId | _: MediatorId => Right(this)
    case _: SequencerId => Left(AuthenticationNotSupportedForMember(member))
  }

  sealed abstract class AuthenticationError(val reason: String, val code: String)
  final case class NoKeysWithCorrectUsageRegistered(
      member: Member,
      usage: NonEmpty[Set[SigningKeyUsage]],
  ) extends AuthenticationError(
        s"Member $member has no keys registered with usage $usage",
        "NoKeysWithCorrectUsageRegistered",
      )
  final case class FailedToSign(member: Member, error: SigningError)
      extends AuthenticationError("Failed to sign nonce", "FailedToSign")
  final case class MissingNonce(member: Member)
      extends AuthenticationError(
        s"Member $member has not been previously assigned a handshake nonce",
        "MissingNonce",
      )
  final case class InvalidSignature(member: Member)
      extends AuthenticationError(
        s"Given signature for member $member is invalid",
        "InvalidSignature",
      )
  final case class MissingToken(member: Member)
      extends AuthenticationError(
        s"Authentication token for member $member has expired. Please reauthenticate.",
        "MissingToken",
      )
  final case class NonMatchingSynchronizerId(member: Member, synchronizerId: SynchronizerId)
      extends AuthenticationError(
        show"Synchronizer id $synchronizerId provided by member $member does not match the synchronizer id of the synchronizer the ${member.description} is trying to connect to",
        "NonMatchingSynchronizerId",
      )
  final case class MemberAccessDisabled(member: Member)
      extends AuthenticationError(
        s"Member $member access is disabled",
        "MemberAccessDisabled",
      )
  final case class TokenVerificationException(member: String)
      extends AuthenticationError(
        s"Due to an internal error, the server side token lookup for member $member failed",
        "VerifyTokenTimeout",
      )
  final case object LogoutTokenDoesNotExist
      extends AuthenticationError(
        s"The token provided for logging out does not exist",
        "LogoutTokenDoesNotExist",
      )

  final case class AuthenticationNotSupportedForMember(member: Member)
      extends AuthenticationError(
        reason = s"Authentication for member type is not supported: $member",
        code = "UnsupportedMember",
      )

  def hashSynchronizerNonce(
      nonce: Nonce,
      synchronizerId: SynchronizerId,
      pureCrypto: CryptoPureApi,
  ): Hash = {
    val builder = commonNonce(pureCrypto, nonce, synchronizerId)
    builder.finish()
  }

  def signSynchronizerNonce(
      member: Member,
      nonce: Nonce,
      synchronizerId: SynchronizerId,
      possibleSigningKeys: NonEmpty[Seq[Fingerprint]],
      crypto: Crypto,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, AuthenticationError, Signature] = {
    val hash = hashSynchronizerNonce(nonce, synchronizerId, crypto.pureCrypto)

    for {
      // see if we have any of the possible keys with the correct usage that could be used to sign
      availableSigningKey <-
        crypto.cryptoPrivateStore
          .filterSigningKeys(
            possibleSigningKeys.forgetNE,
            SigningKeyUsage.SequencerAuthenticationOnly,
          )
          .map(_.headOption) // the first we find is as good as any
          .leftMap(_ =>
            NoKeysWithCorrectUsageRegistered(member, SigningKeyUsage.SequencerAuthenticationOnly)
          )
          .subflatMap(
            _.toRight[AuthenticationError](
              NoKeysWithCorrectUsageRegistered(member, SigningKeyUsage.SequencerAuthenticationOnly)
            )
          )
      sig <- crypto.privateCrypto
        .sign(hash, availableSigningKey, SigningKeyUsage.SequencerAuthenticationOnly)
        .leftMap[AuthenticationError](FailedToSign(member, _))
    } yield sig
  }

  /** Hash the common fields of the nonce. Implementations of MemberAuthentication can then add
    * their own fields as appropriate.
    */
  protected def commonNonce(
      pureApi: CryptoPureApi,
      nonce: Nonce,
      synchronizerId: SynchronizerId,
  ): HashBuilder =
    pureApi
      .build(HashPurpose.AuthenticationToken)
      .addWithoutLengthPrefix(
        nonce.getCryptographicEvidence
      ) // Nonces have a fixed length so it's fine to not add a length prefix
      .add(synchronizerId.toProtoPrimitive)

}
