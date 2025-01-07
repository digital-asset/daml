// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.authentication

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.option.*
import cats.syntax.traverse.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{NonNegativeFiniteDuration, ProcessingTimeout}
import com.digitalasset.canton.crypto.{Crypto, Fingerprint, Nonce}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.SilentLogPolicy
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, GrpcClient}
import com.digitalasset.canton.sequencer.api.v30.SequencerAuthentication.{
  AuthenticateRequest,
  AuthenticateResponse,
  ChallengeRequest,
  ChallengeResponse,
  LogoutRequest,
}
import com.digitalasset.canton.sequencer.api.v30.SequencerAuthenticationServiceGrpc.SequencerAuthenticationServiceStub
import com.digitalasset.canton.sequencing.authentication.grpc.AuthenticationTokenWithExpiry
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.{Member, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry.ErrorKind.{FatalErrorKind, TransientErrorKind}
import com.digitalasset.canton.util.retry.{ErrorKind, ExceptionRetryPolicy, Pause}
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.{Status, StatusRuntimeException}

import scala.concurrent.ExecutionContext

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
    synchronizerId: SynchronizerId,
    member: Member,
    crypto: Crypto,
    supportedProtocolVersions: Seq[ProtocolVersion],
    config: AuthenticationTokenManagerConfig,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  def generateToken(
      authenticationClient: GrpcClient[SequencerAuthenticationServiceStub]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Status, AuthenticationTokenWithExpiry] = {
    def generateTokenET: FutureUnlessShutdown[Either[Status, AuthenticationTokenWithExpiry]] =
      performUnlessClosingUSF(functionFullName) {
        (for {
          challenge <- getChallenge(authenticationClient)
          nonce <- Nonce
            .fromProtoPrimitive(challenge.nonce)
            .leftMap(err => Status.INVALID_ARGUMENT.withDescription(s"Invalid nonce: $err"))
            .toEitherT[FutureUnlessShutdown]
          token <- authenticate(authenticationClient, nonce, challenge.fingerprints)
        } yield token).value
      }

    EitherT {
      Pause(
        logger,
        this,
        maxRetries = config.retries.value,
        delay = config.pauseRetries.underlying,
        operationName = "generate sequencer authentication token",
      ).unlessShutdown(generateTokenET, AuthenticationTokenProvider.exceptionRetryPolicy)
    }
  }

  private def getChallenge(
      authenticationClient: GrpcClient[SequencerAuthenticationServiceStub]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Status, ChallengeResponse] =
    CantonGrpcUtil
      .sendGrpcRequest(authenticationClient, "sequencer-authentication-channel")(
        _.challenge(
          ChallengeRequest(
            member.toProtoPrimitive,
            supportedProtocolVersions.map(_.toProtoPrimitive),
          )
        ),
        "obtain challenge from sequencer",
        timeouts.network.duration,
        logger,
        logPolicy = SilentLogPolicy,
        retryPolicy = _ => false,
      )
      .leftMap(_.status)

  private def authenticate(
      authenticationClient: GrpcClient[SequencerAuthenticationServiceStub],
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
          synchronizerId,
          fingerprintsNel,
          crypto,
        )
        .leftMap(err => Status.INTERNAL.withDescription(err.toString))
      response <- CantonGrpcUtil
        .sendGrpcRequest(authenticationClient, "sequencer-authentication-channel")(
          _.authenticate(
            AuthenticateRequest(
              member = member.toProtoPrimitive,
              signature = signature.toProtoV30.some,
              nonce = nonce.toProtoPrimitive,
            )
          ),
          "authenticate with sequencer",
          timeouts.network.duration,
          logger,
          logPolicy = SilentLogPolicy,
          retryPolicy = _ => false,
        )
        .leftMap(_.status)
      AuthenticateResponse(tokenP, expiryOP) = response
      parsedResponseE = for {
        token <- AuthenticationToken.fromProtoPrimitive(tokenP).leftMap(_.toString)
        expiresAtP <- ProtoConverter.required("expires_at", expiryOP).leftMap(_.toString)
        expiresAt <- CantonTimestamp.fromProtoTimestamp(expiresAtP).leftMap(_.toString)
      } yield AuthenticationTokenWithExpiry(token, expiresAt)
      token <- EitherT.fromEither[FutureUnlessShutdown](
        parsedResponseE.leftMap(err =>
          Status.INTERNAL.withDescription(s"Received invalid authentication token: $err")
        )
      )
    } yield token

  def logout(
      authenticationClient: GrpcClient[SequencerAuthenticationServiceStub]
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, Status, Unit] =
    for {
      // Generate a new token to use as "entry point" to invalidate all tokens
      tokenWithExpiry <- generateToken(authenticationClient)
      token = tokenWithExpiry.token
      _ <- CantonGrpcUtil
        .sendGrpcRequest(authenticationClient, "sequencer-authentication-channel")(
          _.logout(LogoutRequest(token.toProtoPrimitive)),
          "logout from sequencer",
          timeouts.network.duration,
          logger,
          logPolicy = SilentLogPolicy,
        )
        .leftMap(_.status)
    } yield ()
}

object AuthenticationTokenProvider {
  private val exceptionRetryPolicy: ExceptionRetryPolicy =
    new ExceptionRetryPolicy {
      override protected def determineExceptionErrorKind(
          exception: Throwable,
          logger: TracedLogger,
      )(implicit tc: TraceContext): ErrorKind =
        exception match {
          // Ideally we would like to retry only on retryable gRPC status codes (such as `UNAVAILABLE`),
          // but as this could be hard to get right, we compromise by retrying on all gRPC status codes,
          // and use a finite number of retries.
          case _: StatusRuntimeException => TransientErrorKind()
          case _ => FatalErrorKind
        }
    }
}
