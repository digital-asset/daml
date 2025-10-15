// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.authentication

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.option.*
import cats.syntax.traverse.*
import com.daml.metrics.api.MetricsContext
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.digitalasset.canton.config.{
  CantonConfigValidator,
  ProcessingTimeout,
  UniformCantonConfigValidation,
}
import com.digitalasset.canton.crypto.{Fingerprint, Nonce, SynchronizerCrypto}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.metrics.SequencerConnectionPoolMetrics
import com.digitalasset.canton.networking.Endpoint
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
import com.digitalasset.canton.topology.{Member, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry.ErrorKind.{FatalErrorKind, TransientErrorKind}
import com.digitalasset.canton.util.retry.{
  Backoff,
  ErrorKind,
  ExceptionRetryPolicy,
  Jitter,
  Pause,
  RetryWithDelay,
}
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.{Status, StatusRuntimeException}

import scala.concurrent.ExecutionContext

/** Configures authentication token fetching
  *
  * @param refreshAuthTokenBeforeExpiry
  *   how much time before the auth token expires should we fetch a new one?
  */
final case class AuthenticationTokenManagerConfig(
    refreshAuthTokenBeforeExpiry: config.NonNegativeFiniteDuration =
      AuthenticationTokenManagerConfig.defaultRefreshAuthTokenBeforeExpiry,
    retries: NonNegativeInt = AuthenticationTokenManagerConfig.defaultRetries,
    minRetryInterval: config.NonNegativeFiniteDuration =
      AuthenticationTokenManagerConfig.defaultMinRetryInterval,
    backoff: Option[AuthenticationTokenManagerExponentialBackoffConfig] = None,
) extends UniformCantonConfigValidation

final case class AuthenticationTokenManagerExponentialBackoffConfig(
    base: NonNegativeInt = NonNegativeInt.tryCreate(2),
    maxRetryInterval: config.NonNegativeFiniteDuration =
      AuthenticationTokenManagerExponentialBackoffConfig.defaultMaxRetryInterval,
    jitter: Option[AuthenticationTokenManagerExponentialBackoffJitterConfig] =
      AuthenticationTokenManagerExponentialBackoffConfig.defaultJitter,
) extends UniformCantonConfigValidation

sealed trait AuthenticationTokenManagerExponentialBackoffJitterConfig
    extends UniformCantonConfigValidation

object AuthenticationTokenManagerConfig {
  implicit val authenticationTokenManagerConfigCantonConfigValidator
      : CantonConfigValidator[AuthenticationTokenManagerConfig] = {
    import com.digitalasset.canton.config.CantonConfigValidatorInstances.*
    CantonConfigValidatorDerivation[AuthenticationTokenManagerConfig]
  }
  private val defaultRefreshAuthTokenBeforeExpiry: config.NonNegativeFiniteDuration =
    config.NonNegativeFiniteDuration.ofSeconds(20)
  private val defaultRetries: NonNegativeInt = NonNegativeInt.tryCreate(20)
  private val defaultMinRetryInterval: config.NonNegativeFiniteDuration =
    config.NonNegativeFiniteDuration.ofMillis(500)
}

object AuthenticationTokenManagerExponentialBackoffConfig {
  implicit val authenticationTokenManagerExponentialBackoffConfigValidator
      : CantonConfigValidator[AuthenticationTokenManagerExponentialBackoffConfig] = {
    import com.digitalasset.canton.config.CantonConfigValidatorInstances.*
    CantonConfigValidatorDerivation[AuthenticationTokenManagerExponentialBackoffConfig]
  }
  private val defaultMaxRetryInterval: config.NonNegativeFiniteDuration =
    config.NonNegativeFiniteDuration.ofMinutes(2)
  private val defaultJitter: Option[AuthenticationTokenManagerExponentialBackoffJitterConfig] =
    None
}

object AuthenticationTokenManagerExponentialBackoffJitterConfig {
  final case object Equal extends AuthenticationTokenManagerExponentialBackoffJitterConfig
  final case object Full extends AuthenticationTokenManagerExponentialBackoffJitterConfig

  implicit val authenticationTokenManagerExponentialBackoffJitterConfigValidator
      : CantonConfigValidator[AuthenticationTokenManagerExponentialBackoffJitterConfig] =
    CantonConfigValidatorDerivation[AuthenticationTokenManagerExponentialBackoffJitterConfig]
}

/** Fetch an authentication token from the sequencer by using the sequencer authentication service
  */
class AuthenticationTokenProvider(
    synchronizerId: PhysicalSynchronizerId,
    member: Member,
    crypto: SynchronizerCrypto,
    supportedProtocolVersions: Seq[ProtocolVersion],
    config: AuthenticationTokenManagerConfig,
    metricsO: Option[SequencerConnectionPoolMetrics],
    metricsContext: MetricsContext,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  def generateToken(
      endpoint: Endpoint,
      authenticationClient: GrpcClient[SequencerAuthenticationServiceStub],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Status, AuthenticationTokenWithExpiry] = {
    def generateTokenET: FutureUnlessShutdown[Either[Status, AuthenticationTokenWithExpiry]] =
      synchronizeWithClosing(functionFullName) {
        (for {
          challenge <- getChallenge(endpoint, authenticationClient)
          nonce <- Nonce
            .fromProtoPrimitive(challenge.nonce)
            .leftMap(err => Status.INVALID_ARGUMENT.withDescription(s"Invalid nonce: $err"))
            .toEitherT[FutureUnlessShutdown]
          token <- authenticate(endpoint, authenticationClient, nonce, challenge.fingerprints)
        } yield token).value
      }

    val operation = "generate sequencer authentication token"
    val maxRetriesInt = config.retries.value
    val minRetryDuration = config.minRetryInterval.underlying
    val retryWithDelay =
      config.backoff.fold[RetryWithDelay](
        Pause(
          logger,
          this,
          maxRetries = maxRetriesInt,
          delay = minRetryDuration,
          operation,
        )
      ) { backoffConfig =>
        val maxRetryDuration = backoffConfig.maxRetryInterval.underlying
        val baseInt = backoffConfig.base.unwrap
        val jitter =
          backoffConfig.jitter.fold(
            Jitter.none(maxRetryDuration, baseInt)
          ) {
            case AuthenticationTokenManagerExponentialBackoffJitterConfig.Equal =>
              Jitter.equal(maxRetryDuration, base = baseInt)
            case AuthenticationTokenManagerExponentialBackoffJitterConfig.Full =>
              Jitter.full(maxRetryDuration, base = baseInt)
          }
        Backoff(
          logger,
          this,
          maxRetries = maxRetriesInt,
          initialDelay = minRetryDuration,
          maxDelay = maxRetryDuration,
          operationName = operation,
        )(jitter)
      }

    EitherT(
      retryWithDelay.unlessShutdown(
        generateTokenET,
        AuthenticationTokenProvider.exceptionRetryPolicy,
      )
    )
  }

  private def getChallenge(
      endpoint: Endpoint,
      authenticationClient: GrpcClient[SequencerAuthenticationServiceStub],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Status, ChallengeResponse] = {
    metricsO.foreach(
      _.connectionRequests.inc()(metricsContext.withExtraLabels("endpoint" -> "Challenge"))
    )
    CantonGrpcUtil
      .sendGrpcRequest(authenticationClient, s"sequencer-authentication-channel-$endpoint")(
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
  }

  private def authenticate(
      endpoint: Endpoint,
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
        .signSynchronizerNonce(
          member,
          nonce,
          synchronizerId,
          fingerprintsNel,
          crypto,
        )
        .leftMap(err => Status.INTERNAL.withDescription(err.toString))
      _ = metricsO.foreach(
        _.connectionRequests.inc()(metricsContext.withExtraLabels("endpoint" -> "Authenticate"))
      )
      response <- CantonGrpcUtil
        .sendGrpcRequest(authenticationClient, s"sequencer-authentication-channel-$endpoint")(
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
      endpoint: Endpoint,
      authenticationClient: GrpcClient[SequencerAuthenticationServiceStub],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, Status, Unit] =
    for {
      // Generate a new token to use as "entry point" to invalidate all tokens
      tokenWithExpiry <- generateToken(endpoint, authenticationClient)
      token = tokenWithExpiry.token
      _ = metricsO.foreach(
        _.connectionRequests.inc()(metricsContext.withExtraLabels("endpoint" -> "Logout"))
      )
      _ <- CantonGrpcUtil
        .sendGrpcRequest(authenticationClient, s"sequencer-authentication-channel-$endpoint")(
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
          case ex: StatusRuntimeException
              if ex.getStatus.getCode == Status.Code.UNAVAILABLE &&
                (ex.getStatus.getDescription.contains("Channel shutdown invoked") ||
                  ex.getStatus.getDescription.contains("Channel shutdownNow invoked")) =>
            FatalErrorKind

          // Ideally we would like to retry only on retryable gRPC status codes (such as `UNAVAILABLE`),
          // but as this could be hard to get right, we compromise by retrying on all gRPC status codes,
          // and use a finite number of retries.
          case _: StatusRuntimeException => TransientErrorKind()
          case _ => FatalErrorKind
        }
    }
}
