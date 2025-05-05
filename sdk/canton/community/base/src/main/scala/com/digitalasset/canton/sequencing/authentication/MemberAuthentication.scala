// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.authentication

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication.AuthenticationError
import com.digitalasset.canton.sequencing.authentication.grpc.AuthenticationTokenManager
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.{Status, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

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
      crypto: SynchronizerCrypto,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, AuthenticationError, Signature]
}

object MemberAuthentication extends MemberAuthentication {

  import com.digitalasset.canton.util.ShowUtil.*

  def apply(member: Member): Either[AuthenticationError, MemberAuthentication] = member match {
    // TODO(#24306) allow sequencer-sequencer authentication only for BFT sequencers on P2P endpoints
    // Potential new future node types must be explicitly allowed to authenticate
    case _: ParticipantId | _: MediatorId | _: SequencerId => Right(this)
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
      crypto: SynchronizerCrypto,
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
            possibleSigningKeys,
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

  /** Retrieves an authentication token through a
    * [[com.digitalasset.canton.sequencing.authentication.grpc.AuthenticationTokenManager]],
    * converting shutdown events and any exception into a [[io.grpc.Status]].
    */
  def getToken(
      tokenManager: AuthenticationTokenManager
  )(implicit
      ec: ExecutionContext,
      errorLoggingContext: ErrorLoggingContext,
      traceContext: TraceContext,
  ): EitherT[Future, Status, AuthenticationToken] =
    EitherT(
      tokenManager.getToken
        .leftMap(err =>
          Status.PERMISSION_DENIED.withDescription(s"Authentication token refresh error: $err")
        )
        .value
        .onShutdown(
          Left(
            CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
              .Error()
              .asGrpcError
              .getStatus
              .withDescription("Token refresh aborted due to shutdown")
          )
        )
        .recover {
          case grpcError: StatusRuntimeException =>
            // if auth token refresh fails with a grpc error, pass along that status so that the grpc subscription retry
            // mechanism can base the retry decision on it.
            Left(
              grpcError.getStatus
                .withDescription("Authentication token refresh failed with grpc error")
            )
          case NonFatal(ex) =>
            // otherwise indicate internal error
            Left(
              Status.INTERNAL
                .withDescription("Authentication token refresh failed with exception")
                .withCause(ex)
            )
        }
    )

  /** Hash the common fields of the nonce. Implementations of MemberAuthentication can then add
    * their own fields as appropriate.
    */
  private def commonNonce(
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
