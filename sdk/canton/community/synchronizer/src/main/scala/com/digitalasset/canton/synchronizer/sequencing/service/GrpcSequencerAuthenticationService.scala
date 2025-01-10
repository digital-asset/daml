// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.error.{ErrorCategory, ErrorCode, Explanation}
import com.digitalasset.canton.crypto.{Nonce, Signature}
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.error.{Alarm, AlarmErrorCode, CantonError}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcFUSExtended
import com.digitalasset.canton.sequencer.api.v30.SequencerAuthentication.{
  AuthenticateRequest,
  AuthenticateResponse,
  ChallengeRequest,
  ChallengeResponse,
  LogoutRequest,
  LogoutResponse,
}
import com.digitalasset.canton.sequencer.api.v30.SequencerAuthenticationServiceGrpc.SequencerAuthenticationService
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication.{
  AuthenticationError,
  LogoutTokenDoesNotExist,
}
import com.digitalasset.canton.sequencing.authentication.grpc.AuthenticationTokenWithExpiry
import com.digitalasset.canton.sequencing.authentication.{AuthenticationToken, MemberAuthentication}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.synchronizer.Synchronizer.GrpcSequencerAuthenticationErrorGroup
import com.digitalasset.canton.synchronizer.sequencing.authentication.MemberAuthenticationService
import com.digitalasset.canton.synchronizer.sequencing.service.GrpcSequencerAuthenticationService.{
  SequencerAuthenticationFailure,
  SequencerAuthenticationFaultyOrMalicious,
}
import com.digitalasset.canton.synchronizer.service.HandshakeValidator
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}

class GrpcSequencerAuthenticationService(
    authenticationService: MemberAuthenticationService,
    protocolVersion: ProtocolVersion,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SequencerAuthenticationService
    with NamedLogging {

  /** This will complete the participant authentication process using the challenge information and returning a token
    * to be used for further authentication.
    */
  override def authenticate(request: AuthenticateRequest): Future[AuthenticateResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    (for {
      member <- eitherT(deserializeMember(request.member))
      signature <- eitherT(
        ProtoConverter
          .parseRequired(Signature.fromProtoV30, "signature", request.signature)
          .leftMap(err => Status.INVALID_ARGUMENT.withDescription(err.toString))
      )
      providedNonce <- eitherT(
        Nonce
          .fromProtoPrimitive(request.nonce)
          .leftMap(err => Status.INVALID_ARGUMENT.withDescription(err.toString))
      )
      tokenAndExpiry <- authenticationService
        .validateSignature(member, signature, providedNonce)
        .leftMap(handleAuthError)
    } yield {
      val AuthenticationTokenWithExpiry(token, expiry) = tokenAndExpiry
      AuthenticateResponse(
        token = token.toProtoPrimitive,
        expiresAt = Some(expiry.toProtoTimestamp),
      )
    }).valueOr { error =>
      // create error message to appropriately log this error
      val redactedError = if (isSensitive(error)) {
        SequencerAuthenticationFaultyOrMalicious
          .AuthenticationFailure(request.member, error)
          .discard
        error.withDescription("Bad authentication request")
      } else {
        SequencerAuthenticationFailure
          .AuthenticationFailure(request.member, error)
          .discard
        error
      }
      throw redactedError.asRuntimeException()
    }.asGrpcResponse
  }

  /** This will return a random number (nonce) plus the fingerprint of the key the participant needs to use to complete
    * the authentication process with this domain.
    * A handshake check is also done here to make sure that no participant can start authenticating without doing this check.
    * While the pure handshake can be called without any prior setup, this endpoint will only work after topology state
    * for the participant has been pushed to this domain.
    */
  override def challenge(request: ChallengeRequest): Future[ChallengeResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    (for {
      _ <- eitherT(handshakeValidation(request))
      member <- eitherT(deserializeMember(request.member))
      result <- authenticationService
        .generateNonce(member)
        .leftMap(handleAuthError)
    } yield {
      val (nonce, fingerprints) = result
      ChallengeResponse(
        nonce.toProtoPrimitive,
        fingerprints.map(_.unwrap).toList,
      )
    }).valueOr { error =>
      val redactedError = if (isSensitive(error)) {
        SequencerAuthenticationFaultyOrMalicious
          .ChallengeFailure(
            request.member,
            request.memberProtocolVersions,
            error,
          )
          .discard
        error.withDescription("Bad challenge request")
      } else {
        SequencerAuthenticationFailure
          .ChallengeFailure(
            request.member,
            request.memberProtocolVersions,
            error,
          )
          .discard
        error
      }
      throw redactedError.asRuntimeException()
    }.asGrpcResponse
  }

  private def isSensitive(err: Status): Boolean =
    err.getCode == Status.Code.INTERNAL || err.getCode == Status.Code.INVALID_ARGUMENT

  private def handleAuthError(err: AuthenticationError): Status = {
    def maliciousOrFaulty(): Status =
      Status.INTERNAL.withDescription(err.reason)

    err match {
      case MemberAuthentication.MemberAccessDisabled(_) =>
        Status.PERMISSION_DENIED.withDescription(err.reason)
      case MemberAuthentication.NonMatchingSynchronizerId(_, _) =>
        Status.FAILED_PRECONDITION.withDescription(err.reason)
      case MemberAuthentication.NoKeysWithCorrectUsageRegistered(_, _) => maliciousOrFaulty()
      case MemberAuthentication.FailedToSign(_, _) => maliciousOrFaulty()
      case MemberAuthentication.MissingNonce(_) => maliciousOrFaulty()
      case MemberAuthentication.InvalidSignature(_) => maliciousOrFaulty()
      case MemberAuthentication.MissingToken(_) => maliciousOrFaulty()
      case MemberAuthentication.TokenVerificationException(_) => maliciousOrFaulty()
      case MemberAuthentication.AuthenticationNotSupportedForMember(_) => maliciousOrFaulty()
      case MemberAuthentication.LogoutTokenDoesNotExist => maliciousOrFaulty()
    }
  }

  private def eitherT[A, B](value: Either[A, B]) = EitherT.fromEither[FutureUnlessShutdown](value)

  private def deserializeMember(
      memberPO: String
  ): Either[Status, Member] =
    Member
      .fromProtoPrimitive(memberPO, "member")
      .leftMap(err =>
        Status.INVALID_ARGUMENT.withDescription(s"Failed to deserialize member: $err")
      )

  private def handshakeValidation(request: ChallengeRequest): Either[Status, Unit] =
    HandshakeValidator
      .clientIsCompatible(protocolVersion, request.memberProtocolVersions, minClientVersionP = None)
      .leftMap(err => Status.FAILED_PRECONDITION.withDescription(err))

  /** Unconditionally revoke a member's authentication tokens and disconnect it
    */
  override def logout(request: LogoutRequest): Future[LogoutResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val result = for {
      providedToken <- AuthenticationToken.fromProtoPrimitive(request.token) match {
        case Right(token) => FutureUnlessShutdown.pure(token)
        case Left(err) =>
          FutureUnlessShutdown.failed(
            Status.INVALID_ARGUMENT
              .withDescription(s"Failed to deserialize token: $err")
              .asRuntimeException()
          )
      }
      logoutResult <- authenticationService.invalidateMemberWithToken(providedToken)
      _ <- logoutResult match {
        case Right(()) => FutureUnlessShutdown.pure(())
        case Left(err @ LogoutTokenDoesNotExist) =>
          FutureUnlessShutdown.failed(
            Status.FAILED_PRECONDITION
              .withDescription(s"Failed to logout: $err")
              .asRuntimeException()
          )
      }
    } yield ()

    result.map(_ => LogoutResponse()).asGrpcResponse
  }
}

object GrpcSequencerAuthenticationService extends GrpcSequencerAuthenticationErrorGroup {

  @Explanation(
    """This error indicates that a client failed to authenticate with the sequencer. The message is logged
      |on the server in order to support an operator to provide explanations to clients struggling to connect."""
  )
  object SequencerAuthenticationFailure
      extends ErrorCode(
        id = "CLIENT_AUTHENTICATION_REJECTED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {

    override protected def exposedViaApi: Boolean = false

    final case class ChallengeFailure(
        member: String,
        supportedProtocol: Seq[Int],
        response: Status,
    )(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause =
            s"Challenge for $member rejected with ${response.getCode}/${response.getDescription}"
        )
        with CantonError

    final case class AuthenticationFailure(member: String, response: Status)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Authentication for $member rejected with ${response.getCode}/${response.getDescription}"
        )
        with CantonError
  }

  @Explanation(
    """This error indicates that a client failed to authenticate with the sequencer due to a reason possibly
      |pointing out to faulty or malicious behaviour. The message is logged on the server in order to support an
      |operator to provide explanations to clients struggling to connect."""
  )
  object SequencerAuthenticationFaultyOrMalicious
      extends AlarmErrorCode(id = "CLIENT_AUTHENTICATION_FAULTY") {

    override protected def exposedViaApi: Boolean = false

    final case class ChallengeFailure(
        member: String,
        supportedProtocol: Seq[Int],
        response: Status,
    )(implicit override val loggingContext: ErrorLoggingContext)
        extends Alarm(
          cause =
            s"Faulty or malicious challenge for $member rejected with ${response.getCode}/${response.getDescription}"
        )
        with CantonError

    final case class AuthenticationFailure(member: String, response: Status)(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends Alarm(
          cause =
            s"Faulty or malicious authentication for $member rejected with ${response.getCode}/${response.getDescription}"
        )
        with CantonError
  }
}
