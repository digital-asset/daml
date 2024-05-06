// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.authentication

import cats.data.EitherT
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication.AuthenticationError
import com.digitalasset.canton.topology.{DomainId, *}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

trait MemberAuthentication {

  def hashDomainNonce(
      nonce: Nonce,
      domainId: DomainId,
      pureCrypto: CryptoPureApi,
  ): Hash

  /** Member concatenates the nonce with the domain's id and signs it (step 3)
    */
  def signDomainNonce(
      member: Member,
      nonce: Nonce,
      domainId: DomainId,
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
    case _: UnauthenticatedMemberId => Left(AuthenticationNotSupportedForMember(member))
  }

  sealed abstract class AuthenticationError(val reason: String, val code: String)
  final case class NoKeysRegistered(member: Member)
      extends AuthenticationError(s"Member $member has no keys registered", "NoKeysRegistered")
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
  final case class NonMatchingDomainId(member: Member, domainId: DomainId)
      extends AuthenticationError(
        show"Domain id $domainId provided by member $member does not match the domain id of the domain the ${member.description} is trying to connect to",
        "NonMatchingDomainId",
      )
  final case class ParticipantAccessDisabled(participantId: ParticipantId)
      extends AuthenticationError(
        s"Participant $participantId access is disabled",
        "ParticipantAccessDisabled",
      )

  final case class MediatorAccessDisabled(mediator: MediatorId)
      extends AuthenticationError(
        s"Mediator $mediator access is disabled",
        "MediatorAccessDisabled",
      )

  final case class TokenVerificationException(member: String)
      extends AuthenticationError(
        s"Due to an internal error, the server side token lookup for member $member failed",
        "VerifyTokenTimeout",
      )

  final case class AuthenticationNotSupportedForMember(member: Member)
      extends AuthenticationError(
        reason = s"Authentication for member type is not supported: $member",
        code = "UnsupportedMember",
      )
  final object PassiveSequencer
      extends AuthenticationError(
        reason =
          "Sequencer is currently passive. Connect to a different sequencer and retry the request or wait for the sequencer to become active again.",
        code = "PassiveSequencer",
      )

  def hashDomainNonce(
      nonce: Nonce,
      domainId: DomainId,
      pureCrypto: CryptoPureApi,
  ): Hash = {
    val builder = commonNonce(pureCrypto, nonce, domainId)
    builder.finish()
  }

  def signDomainNonce(
      member: Member,
      nonce: Nonce,
      domainId: DomainId,
      possibleSigningKeys: NonEmpty[Seq[Fingerprint]],
      crypto: Crypto,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, AuthenticationError, Signature] = {
    val hash = hashDomainNonce(nonce, domainId, crypto.pureCrypto)

    for {
      // see if we have any of the possible keys that could be used to sign
      availableSigningKey <- possibleSigningKeys.forgetNE
        .parFilterA(key => crypto.cryptoPrivateStore.existsSigningKey(key))
        .map(_.headOption) // the first we find is as good as any
        .leftMap(_ => NoKeysRegistered(member))
        .subflatMap(_.toRight[AuthenticationError](NoKeysRegistered(member)))
      sig <- crypto.privateCrypto
        .sign(hash, availableSigningKey)
        .leftMap[AuthenticationError](FailedToSign(member, _))
    } yield sig
  }

  /** Hash the common fields of the nonce.
    * Implementations of MemberAuthentication can then add their own fields as appropriate.
    */
  protected def commonNonce(pureApi: CryptoPureApi, nonce: Nonce, domainId: DomainId): HashBuilder =
    pureApi
      .build(HashPurpose.AuthenticationToken)
      .addWithoutLengthPrefix(
        nonce.getCryptographicEvidence
      ) // Nonces have a fixed length so it's fine to not add a length prefix
      .add(domainId.toProtoPrimitive)

}
