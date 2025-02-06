// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import cats.Functor
import cats.data.EitherT
import cats.implicits.*
import com.daml.error.{ErrorCategory, ErrorCategoryRetry, ErrorCode, Explanation, Resolution}
import com.daml.grpc.AuthCallCredentials
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.connection.v30.{ApiInfoServiceGrpc, GetApiInfoRequest}
import com.digitalasset.canton.error.CantonErrorGroups.GrpcErrorGroup
import com.digitalasset.canton.error.{BaseCantonError, CantonError}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, OnShutdownRunner, UnlessShutdown}
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{DelayUtil, EitherTUtil}
import com.digitalasset.canton.{GrpcServiceInvocationMethod, ProtoDeserializationError, config}
import io.grpc.*
import io.grpc.Context.CancellableContext
import io.grpc.stub.{AbstractStub, StreamObserver}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object CantonGrpcUtil {
  def wrapErrUS[T](value: ParsingResult[T])(implicit
      loggingContext: ErrorLoggingContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, CantonError, T] =
    wrapErrUS(EitherT.fromEither[FutureUnlessShutdown](value))
  def wrapErrUS[T](value: EitherT[FutureUnlessShutdown, ProtoDeserializationError, T])(implicit
      loggingContext: ErrorLoggingContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, CantonError, T] =
    value.leftMap(x => ProtoDeserializationError.ProtoDeserializationFailure.Wrap(x): CantonError)

  def mapErrNew[T <: CantonError, C](value: Either[T, C])(implicit
      ec: ExecutionContext
  ): EitherT[Future, StatusRuntimeException, C] =
    EitherT.fromEither[Future](value).leftMap(_.asGrpcError)

  def mapErrNewETUS[T <: BaseCantonError, C](value: EitherT[FutureUnlessShutdown, T, C])(implicit
      ec: ExecutionContext,
      errorLoggingContext: ErrorLoggingContext,
  ): EitherT[Future, StatusRuntimeException, C] =
    value.onShutdown(Left(AbortedDueToShutdown.Error())).leftMap(_.asGrpcError)

  def shutdownAsGrpcError[C](
      value: FutureUnlessShutdown[C]
  )(implicit ec: ExecutionContext, errorLoggingContext: ErrorLoggingContext): Future[C] =
    value.onShutdown(throw AbortedDueToShutdown.Error().asGrpcError)

  def shutdownAsGrpcErrorE[A, B](value: EitherT[FutureUnlessShutdown, A, B])(implicit
      ec: ExecutionContext,
      errorLoggingContext: ErrorLoggingContext,
  ): EitherT[Future, A, B] =
    value.onShutdown(throw AbortedDueToShutdown.Error().asGrpcError)

  def mapErrNew[T <: BaseCantonError, C](value: EitherT[Future, T, C])(implicit
      executionContext: ExecutionContext,
      errorLoggingContext: ErrorLoggingContext,
  ): Future[C] =
    EitherTUtil.toFuture(value.leftMap(_.asGrpcError))

  def mapErrNew[T <: CantonError, C](value: EitherT[Future, T, C])(implicit
      ec: ExecutionContext
  ): Future[C] =
    EitherTUtil.toFuture(value.leftMap(_.asGrpcError))

  def mapErrNewEUS[T <: BaseCantonError, C](value: EitherT[FutureUnlessShutdown, T, C])(implicit
      ec: ExecutionContext,
      errorLoggingContext: ErrorLoggingContext,
  ): Future[C] =
    EitherTUtil.toFuture(mapErrNewETUS(value))

  /** Wrapper method for sending a Grpc request.
    * Takes care of appropriate logging and retrying.
    *
    * NOTE that this will NOT WORK for requests with streamed responses, as such requests will report errors to the
    * corresponding [[io.grpc.stub.StreamObserver]]. You need to do error handling within the corresponding
    * [[io.grpc.stub.StreamObserver]].
    *
    * @param client             the Grpc client used to send the request
    * @param serverName         used for logging
    * @param send               the client method for sending the request
    * @param requestDescription used for logging
    * @param timeout            determines how long to retry or wait for a response.
    *                           Will retry until 70% of this timeout has elapsed.
    *                           Will wait for a response until this timeout has elapsed.
    * @param logPolicy          use this to configure log levels for errors
    * @param retryPolicy        invoked after an error to determine whether to retry
    */
  @GrpcServiceInvocationMethod
  def sendGrpcRequest[Svc <: AbstractStub[Svc], Res](client: GrpcClient[Svc], serverName: String)(
      send: Svc => Future[Res],
      requestDescription: String,
      timeout: Duration,
      logger: TracedLogger,
      logPolicy: GrpcLogPolicy = DefaultGrpcLogPolicy,
      retryPolicy: GrpcError => Boolean = _.retry,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, GrpcError, Res] = {
    implicit val ec: ExecutionContext = DirectExecutionContext(logger)

    // depending on the desired timeout, use a deadline or not
    val (clientWithDeadline, calcEffectiveBackoff) = timeout match {
      case finite: FiniteDuration =>
        // The deadline for individual requests.
        val requestDeadline = Deadline.after(finite.length, finite.unit)
        // After this deadline, we won't retry anymore.
        // This deadline is significantly before `requestDeadline`, because we want to avoid DEADLINE_EXCEEDED due to overly short deadlines.
        val retryDeadline = requestDeadline.offset(-finite.toMillis * 3 / 10, TimeUnit.MILLISECONDS)
        (
          client.service.withDeadline(requestDeadline),
          (backoffMs: Long) =>
            Math.min(backoffMs, retryDeadline.timeRemaining(TimeUnit.MILLISECONDS)),
        )
      case Duration.Inf =>
        (client.service, Predef.identity[Long])
      case _ =>
        logger.error(s"Ignoring unexpected timeout $timeout value.")
        (client.service, Predef.identity[Long])
    }

    def go(backoffMs: Long): FutureUnlessShutdown[Either[GrpcError, Res]] =
      if (client.onShutdownRunner.isClosing) FutureUnlessShutdown.abortedDueToShutdown
      else {
        logger.debug(s"Sending request $requestDescription to $serverName.")
        val sendF = sendGrpcRequestUnsafe(clientWithDeadline)(send)
        val withRetries = sendF.transformWith {
          case Success(value) =>
            logger.debug(s"Request $requestDescription has succeeded for $serverName.")
            FutureUnlessShutdown.pure(Right(value)).unwrap
          case Failure(e: StatusRuntimeException) =>
            val error = GrpcError(requestDescription, serverName, e)
            if (client.onShutdownRunner.isClosing) {
              logger.info(s"Ignoring gRPC error due to shutdown. Ignored error: $error")
              FutureUnlessShutdown.abortedDueToShutdown.unwrap
            } else {
              logPolicy.log(error, logger)
              if (retryPolicy(error)) {
                val effectiveBackoff = calcEffectiveBackoff(backoffMs)
                if (effectiveBackoff > 0) {
                  logger.info(s"Waiting for ${effectiveBackoff}ms before retrying...")
                  DelayUtil
                    .delayIfNotClosing(
                      s"Delay retrying request $requestDescription for $serverName",
                      FiniteDuration.apply(effectiveBackoff, TimeUnit.MILLISECONDS),
                      client.onShutdownRunner,
                    )
                    .flatMap { _ =>
                      logger.info(s"Retrying request $requestDescription for $serverName...")
                      go(backoffMs * 2)
                    }
                    .unwrap
                } else {
                  logger.warn("Retry timeout has elapsed, giving up.")
                  FutureUnlessShutdown.pure(Left(error)).unwrap
                }
              } else {
                logger.debug(
                  s"Retry has not been configured for ${error.getClass.getSimpleName}, giving up."
                )
                FutureUnlessShutdown.pure(Left(error)).unwrap
              }
            }
          case Failure(e) =>
            logger.error(
              s"An unexpected exception has occurred while sending request $requestDescription to $serverName.",
              e,
            )
            Future.failed(e)
        }
        FutureUnlessShutdown(withRetries)
      }

    EitherT(go(1))
  }

  /** Method to create a grpc channel and send a single request
    *
    * Based on [[sendGrpcRequest]]
    */
  @GrpcServiceInvocationMethod
  def sendSingleGrpcRequest[Svc <: AbstractStub[Svc], Res](
      serverName: String,
      requestDescription: String,
      channelBuilder: ManagedChannelBuilderProxy,
      stubFactory: Channel => Svc,
      timeout: Duration,
      logger: TracedLogger,
      onShutdownRunner: OnShutdownRunner,
      logPolicy: GrpcLogPolicy = DefaultGrpcLogPolicy,
      retryPolicy: GrpcError => Boolean,
      token: Option[String],
  )(
      send: Svc => Future[Res]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, GrpcError, Res] = {
    val (_, fut) = sendSingleGrpcRequestInternal(
      serverName,
      requestDescription,
      channelBuilder,
      stubFactory,
      timeout,
      logger,
      onShutdownRunner,
      logPolicy,
      retryPolicy,
      token,
    )(send)
    fut
  }

  @GrpcServiceInvocationMethod
  private def sendSingleGrpcRequestInternal[Svc <: AbstractStub[Svc], Res](
      serverName: String,
      requestDescription: String,
      channelBuilder: ManagedChannelBuilderProxy,
      stubFactory: Channel => Svc,
      timeout: Duration,
      logger: TracedLogger,
      onShutdownRunner: OnShutdownRunner,
      logPolicy: GrpcLogPolicy,
      retryPolicy: GrpcError => Boolean,
      token: Option[String],
  )(send: Svc => Future[Res])(implicit
      traceContext: TraceContext
  ): (GrpcManagedChannelHandle, EitherT[FutureUnlessShutdown, GrpcError, Res]) = {
    val managedChannel = GrpcManagedChannel(
      "sendSingleGrpcRequest",
      channelBuilder.build(),
      onShutdownRunner,
      logger,
    )
    val client = GrpcClient.create(
      managedChannel,
      channel => token.foldLeft(stubFactory(channel))(AuthCallCredentials.authorizingStub),
    )

    val res = sendGrpcRequest(client, serverName)(
      send(_),
      requestDescription,
      timeout,
      logger,
      logPolicy,
      retryPolicy,
    )

    implicit val ec: ExecutionContext = DirectExecutionContext(logger)
    managedChannel.handle -> res.thereafter { _ =>
      managedChannel.close()
    }
  }

  /** Performs `send` once on `service` after having set the trace context in gRPC context.
    * Does not perform any error handling.
    *
    * Prefer [[sendGrpcRequest]] whenever possible
    */
  @GrpcServiceInvocationMethod
  def sendGrpcRequestUnsafe[Svc <: AbstractStub[Svc], Resp](service: Svc)(
      send: Svc => Future[Resp]
  )(implicit traceContext: TraceContext): Future[Resp] =
    TraceContextGrpc.withGrpcContext(traceContext)(send(service))

  /** Makes the server-streaming call via `send` on the `client` in a fresh cancellable gRPC [[io.grpc.Context]]
    * that is used to construct the stream observer via the `observerFactory`.
    *
    * @param observerFactory Factory to create the stream observer for handling the message stream from the server.
    * @param getObserver Extracts the actual stream observer from the `HasObserver` instance.
    */
  def serverStreamingRequest[Svc <: AbstractStub[Svc], HasObserver, Resp](
      client: GrpcClient[Svc],
      observerFactory: (CancellableContext, OnShutdownRunner) => HasObserver,
  )(getObserver: HasObserver => StreamObserver[Resp])(
      send: (Svc, StreamObserver[Resp]) => Unit
  )(implicit traceContext: TraceContext): HasObserver = {
    // we intentionally don't use `Context.current()` as we don't want to inherit the
    // cancellation scope from upstream requests
    val context: CancellableContext = Context.ROOT.withCancellation()

    val result = observerFactory(context, client.channel)
    val observer = getObserver(result)

    context.run(() =>
      // This trace context will only be used for the initial request, not for the streaming responses
      // Clients must manage the trace context inside the streaming responses themselves
      TraceContextGrpc.withGrpcContext(traceContext) {
        send(client.service, observer)
      }
    )

    result
  }

  /** Makes the bidirectional-streaming call via `send` on the `client` in a fresh cancellable gRPC [[io.grpc.Context]]
    * that is used to construct the stream observer via the `observerFactory`.
    *
    * @param observerFactory Factory to create the stream observer for handling the message stream from the server.
    * @param getObserver Extracts the actual stream observer from the `HasObserver` instance.
    * @tparam F The effect type of the observer factory.
    */
  @GrpcServiceInvocationMethod
  def bidirectionalStreamingRequest[Svc <: AbstractStub[Svc], F[_], HasObserver, Req, Resp](
      client: GrpcClient[Svc],
      observerFactory: (CancellableContext, OnShutdownRunner) => F[HasObserver],
  )(getObserver: HasObserver => StreamObserver[Resp])(
      send: (Svc, StreamObserver[Resp]) => StreamObserver[Req]
  )(implicit traceContext: TraceContext, F: Functor[F]): F[(HasObserver, StreamObserver[Req])] = {
    // we intentionally don't use `Context.current()` as we don't want to inherit the
    // cancellation scope from upstream requests
    val context: CancellableContext = Context.ROOT.withCancellation()

    val resultF = observerFactory(context, client.channel)
    F.map(resultF) { result =>
      val responseObserver = getObserver(result)

      val requestObserver = context.call(() =>
        // This trace context will only be used for setting up the bidirectional channel.
        // Clients must manage the trace context inside the streaming requests and responses themselves
        TraceContextGrpc.withGrpcContext(traceContext) {
          send(client.service, responseObserver)
        }
      )
      (result, requestObserver)
    }
  }

  trait GrpcLogPolicy {
    def log(error: GrpcError, logger: TracedLogger)(implicit
        traceContext: TraceContext
    ): Unit
  }
  object DefaultGrpcLogPolicy extends GrpcLogPolicy {
    override def log(error: GrpcError, logger: TracedLogger)(implicit
        traceContext: TraceContext
    ): Unit = error.log(logger)
  }
  object SilentLogPolicy extends GrpcLogPolicy {
    def log(error: GrpcError, logger: TracedLogger)(implicit
        traceContext: TraceContext
    ): Unit =
      // Log an info, if a cause is defined to not discard the cause information
      Option(error.status.getCause).foreach { cause =>
        logger.info(error.toString, cause)
      }
  }

  object RetryPolicy {
    lazy val noRetry: GrpcError => Boolean = _ => false
  }

  /** The name of the service that is associated with the sequencer servers' health status.
    * This name can have no relation with the gRPC services that the server is running with, and can be anything
    * as long as the client and servers use the same value.
    */
  val sequencerHealthCheckServiceName = "sequencer-health-check-service"

  object GrpcErrors extends GrpcErrorGroup {

    /** Canton Error that can be used in Grpc Services to signal that a request could not be processed
      * successfully due to the node shutting down
      */
    @Explanation(
      "This error is returned when processing of the request was aborted due to the node shutting down."
    )
    @Resolution(
      "Retry the request against an active and available node."
    )
    object AbortedDueToShutdown
        extends ErrorCode(
          id = "ABORTED_DUE_TO_SHUTDOWN",
          ErrorCategory.TransientServerFailure,
        ) {
      final case class Error()(implicit val loggingContext: ErrorLoggingContext)
          extends CantonError.Impl("request aborted due to shutdown") {
        import scala.concurrent.duration.*
        // Processing may have been cancelled due to a transient error, e.g., server restarting
        // The transient errors might be solved by the application retrying with a higher timeout than
        // The non-transient errors will require operator intervention
        override def retryable = Some(ErrorCategoryRetry(1.minute))
      }
    }
  }

  implicit class GrpcFUSExtended[A](val f: FutureUnlessShutdown[A]) extends AnyVal {
    def asGrpcResponse(implicit ec: ExecutionContext, elc: ErrorLoggingContext): Future[A] =
      f.failOnShutdownTo(GrpcErrors.AbortedDueToShutdown.Error().asGrpcError)
  }

  implicit class GrpcETFUSExtended[A](
      val et: EitherT[FutureUnlessShutdown, StatusRuntimeException, A]
  ) extends AnyVal {
    def asGrpcResponse(implicit ec: ExecutionContext, elc: ErrorLoggingContext): Future[A] =
      EitherTUtil.toFutureUnlessShutdown(et).asGrpcResponse
  }

  implicit class GrpcUSExtended[A](val u: UnlessShutdown[A]) extends AnyVal {
    def asGrpcResponse(implicit elc: ErrorLoggingContext): A =
      u.onShutdown(throw GrpcErrors.AbortedDueToShutdown.Error().asGrpcError)
  }

  def checkCantonApiInfo(
      serverName: String,
      expectedName: String,
      channelBuilder: ManagedChannelBuilderProxy,
      logger: TracedLogger,
      timeout: config.NonNegativeDuration,
      onShutdownRunner: OnShutdownRunner,
      token: Option[String],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val (channelHandle, sendF) = CantonGrpcUtil
      .sendSingleGrpcRequestInternal(
        serverName = s"$serverName/$expectedName",
        requestDescription = "GetApiInfo",
        channelBuilder = channelBuilder,
        stubFactory = ApiInfoServiceGrpc.stub,
        timeout = timeout.unwrap,
        logger = logger,
        logPolicy = CantonGrpcUtil.SilentLogPolicy,
        retryPolicy = CantonGrpcUtil.RetryPolicy.noRetry,
        onShutdownRunner = onShutdownRunner,
        token = token,
      )(_.getApiInfo(GetApiInfoRequest()))
    for {
      apiInfo <- sendF.bimap(_.toString, _.name)
      _ <-
        EitherTUtil.condUnitET[FutureUnlessShutdown](
          apiInfo == expectedName,
          s"Endpoint '${channelHandle.toString}' provides '$apiInfo', " +
            s"expected '$expectedName'. This message indicates a possible mistake in configuration, " +
            s"please check node connection settings for '$serverName'.",
        )
    } yield ()
  }

  object ApiName {
    val AdminApi: String = "admin-api"
    val LedgerApi: String = "ledger-api"
    val SequencerPublicApi: String = "sequencer-public-api"
  }
}
