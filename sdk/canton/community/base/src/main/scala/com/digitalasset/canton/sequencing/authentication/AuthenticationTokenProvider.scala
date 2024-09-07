// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.authentication

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.option.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{NonNegativeFiniteDuration, ProcessingTimeout}
import com.digitalasset.canton.crypto.{Crypto, Fingerprint, Nonce}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.api.v30.SequencerAuthentication.{
  AuthenticateRequest,
  AuthenticateResponse,
  ChallengeRequest,
  ChallengeResponse,
  LogoutRequest,
}
import com.digitalasset.canton.domain.api.v30.SequencerAuthenticationServiceGrpc.SequencerAuthenticationServiceStub
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.sequencing.authentication.grpc.AuthenticationTokenWithExpiry
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.{DomainId, Member}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.retry.ErrorKind.{FatalErrorKind, TransientErrorKind}
import com.digitalasset.canton.util.retry.{ErrorKind, ExceptionRetryPolicy, Pause}
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.{Status, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Configures authentication token fetching
  *
  * @param refreshAuthTokenBeforeExpiry how much time before the auth token expires should we fetch a new one?
  */
final case class AuthenticationTokenManagerConfig(
    refreshAuthTokenBeforeExpiry: NonNegativeFiniteDuration =
      AuthenticationTokenManagerConfig.defaultRefreshAuthTokenBeforeExpiry,
    retries: NonNegativeInt = AuthenticationTokenManagerConfig.defaultRetries,
    pauseRetries: NonNegativeFiniteDuration = AuthenticationTokenManagerConfig.defaultPauseRetries,
)
object AuthenticationTokenManagerConfig {
  val defaultRefreshAuthTokenBeforeExpiry: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(20)
  val defaultRetries: NonNegativeInt = NonNegativeInt.tryCreate(20)
  val defaultPauseRetries: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMillis(500)
}

/** Fetch an authentication token from the sequencer by using the sequencer authentication service */
class AuthenticationTokenProvider(
    domainId: DomainId,
    member: Member,
    crypto: Crypto,
    supportedProtocolVersions: Seq[ProtocolVersion],
    config: AuthenticationTokenManagerConfig,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  private def shutdownStatus =
    Status.CANCELLED.withDescription("Aborted fetching token due to my node shutdown")

  def generateToken(
      authenticationClient: SequencerAuthenticationServiceStub
  ): EitherT[FutureUnlessShutdown, Status, AuthenticationTokenWithExpiry] = {
    // this should be called by a grpc client interceptor
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    performUnlessClosingEitherU(functionFullName) {
      def generateTokenET: FutureUnlessShutdown[Either[Status, AuthenticationTokenWithExpiry]] =
        (for {
          challenge <- getChallenge(authenticationClient).mapK(FutureUnlessShutdown.outcomeK)
          nonce <- Nonce
            .fromProtoPrimitive(challenge.nonce)
            .leftMap(err => Status.INVALID_ARGUMENT.withDescription(s"Invalid nonce: $err"))
            .toEitherT[FutureUnlessShutdown]
          token <- authenticate(authenticationClient, nonce, challenge.fingerprints)
        } yield token).value

      EitherT {
        Pause(
          logger,
          this,
          maxRetries = config.retries.value,
          delay = config.pauseRetries.underlying,
          operationName = "generate sequencer authentication token",
        ).unlessShutdown(
          generateTokenET,
          new ExceptionRetryPolicy {
            override protected def determineExceptionErrorKind(
                exception: Throwable,
                logger: TracedLogger,
            )(implicit
                tc: TraceContext
            ): ErrorKind =
              exception match {
                // Ideally we would like to retry only on retryable gRPC status codes (such as `UNAVAILABLE`),
                // but as this could be hard to get right, we compromise by retrying on all gRPC status codes,
                // and use a finite number of retries.
                case _: StatusRuntimeException => TransientErrorKind()
                case _ => FatalErrorKind
              }
          },
        ).onShutdown(Left(shutdownStatus))
      }
    }
  }

  private def getChallenge(
      authenticationClient: SequencerAuthenticationServiceStub
  ): EitherT[Future, Status, ChallengeResponse.Success] = EitherT {
    authenticationClient
      .challenge(
        ChallengeRequest(
          member.toProtoPrimitive,
          supportedProtocolVersions.map(_.toProtoPrimitive),
        )
      )
      .map(response => response.value)
      .map {
        case ChallengeResponse.Value.Success(success) => Right(success)
        case ChallengeResponse.Value.Failure(ChallengeResponse.Failure(code, reason)) =>
          Left(Status.fromCodeValue(code).withDescription(reason))
        case ChallengeResponse.Value.Empty =>
          Left(
            Status.INTERNAL.withDescription(
              "Problem with domain handshake with challenge. Received empty response from domain."
            )
          )
      }
  }
  import cats.syntax.traverse.*
  private def authenticate(
      authenticationClient: SequencerAuthenticationServiceStub,
      nonce: Nonce,
      fingerprintsP: Seq[String],
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, Status, AuthenticationTokenWithExpiry] =
    for {
      fingerprintsValid <- fingerprintsP
        .traverse(Fingerprint.fromProtoPrimitive)
        .leftMap(err => Status.INVALID_ARGUMENT.withDescription(err.toString))
        .toEitherT[FutureUnlessShutdown]
      fingerprintsNel <- NonEmpty
        .from(fingerprintsValid)
        .toRight(
          Status.INVALID_ARGUMENT
            .withDescription(s"Failed to deserialize fingerprints $fingerprintsP")
        )
        .toEitherT[FutureUnlessShutdown]
      signature <- MemberAuthentication
        .signDomainNonce(
          member,
          nonce,
          domainId,
          fingerprintsNel,
          crypto,
        )
        .leftMap(err => Status.INTERNAL.withDescription(err.toString))
      token <- EitherT {
        authenticationClient
          .authenticate(
            AuthenticateRequest(
              member = member.toProtoPrimitive,
              signature = signature.toProtoV30.some,
              nonce = nonce.toProtoPrimitive,
            )
          )
          .map(response => response.value)
          .map {
            case AuthenticateResponse.Value.Success(
                  AuthenticateResponse.Success(tokenP, expiryOP)
                ) =>
              (for {
                token <- AuthenticationToken.fromProtoPrimitive(tokenP).leftMap(_.toString)
                expiresAtP <- ProtoConverter.required("expires_at", expiryOP).leftMap(_.toString)
                expiresAt <- CantonTimestamp.fromProtoTimestamp(expiresAtP).leftMap(_.toString)
              } yield AuthenticationTokenWithExpiry(token, expiresAt))
                .leftMap(err =>
                  Status.INTERNAL.withDescription(s"Received invalid authentication token: $err")
                )
            case AuthenticateResponse.Value.Failure(AuthenticateResponse.Failure(code, reason)) =>
              Left(Status.fromCodeValue(code).withDescription(reason))
            case AuthenticateResponse.Value.Empty =>
              Left(
                Status.INTERNAL.withDescription(
                  "Problem authenticating participant. Received empty response from domain."
                )
              )
          }
      }.mapK(FutureUnlessShutdown.outcomeK)
    } yield token

  def logout(
      authenticationClient: SequencerAuthenticationServiceStub
  ): EitherT[FutureUnlessShutdown, Status, Unit] =
    for {
      // Generate a new token to use as "entry point" to invalidate all tokens
      tokenWithExpiry <- generateToken(authenticationClient)
      token = tokenWithExpiry.token

      _ <- EitherT(
        authenticationClient
          .logout(LogoutRequest(token.toProtoPrimitive))
          .transform {
            case Failure(exc: StatusRuntimeException) => Success(Left(exc.getStatus))
            case Failure(exc) => Success(Left(Status.INTERNAL.withDescription(exc.getMessage)))
            case Success(_) => Success(Right(()))
          }
      ).mapK(FutureUnlessShutdown.outcomeK)
    } yield ()
}
