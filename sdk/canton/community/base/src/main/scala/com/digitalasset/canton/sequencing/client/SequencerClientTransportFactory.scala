// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory}
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.client.SequencerClientTransportFactory.ValidateTransportResult
import com.digitalasset.canton.sequencing.client.grpc.GrpcSequencerChannelBuilder
import com.digitalasset.canton.sequencing.client.transports.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.{TraceContext, TracingConfig}
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.retry.RetryUtil.NoExnRetryable
import io.grpc.ConnectivityState
import org.apache.pekko.stream.Materializer

import scala.concurrent.*

trait SequencerClientTransportFactory {

  def makeTransport(
      sequencerConnections: SequencerConnections,
      member: Member,
      requestSigner: RequestSigner,
  )(implicit
      executionContext: ExecutionContextExecutor,
      executionSequencerFactory: ExecutionSequencerFactory,
      materializer: Materializer,
      traceContext: TraceContext,
  ): EitherT[Future, String, NonEmpty[Map[SequencerAlias, SequencerClientTransport]]] =
    MonadUtil
      .sequentialTraverse(sequencerConnections.connections)(conn =>
        makeTransport(conn, member, requestSigner)
          .map(transport => conn.sequencerAlias -> transport)
      )
      .map(transports => NonEmptyUtil.fromUnsafe(transports.toMap))

  def validateTransport(
      sequencerConnections: SequencerConnections,
      logWarning: Boolean,
  )(implicit
      executionContext: ExecutionContextExecutor,
      errorLoggingContext: ErrorLoggingContext,
      closeContext: CloseContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    MonadUtil
      .sequentialTraverse(sequencerConnections.connections)(conn =>
        validateTransport(conn, logWarning)
          .transform {
            case Right(_) => Right(ValidateTransportResult.Valid)
            case Left(error) => Right(ValidateTransportResult.NotValid(error))
          }
      )
      .flatMap(checkAgainstTrustThreshold(sequencerConnections.sequencerTrustThreshold, _))

  private def checkAgainstTrustThreshold(
      sequencerTrustThreshold: PositiveInt,
      results: Seq[ValidateTransportResult],
  )(implicit
      executionContext: ExecutionContextExecutor
  ): EitherT[FutureUnlessShutdown, String, Unit] = EitherT.fromEither[FutureUnlessShutdown] {
    if (results.count(_ == ValidateTransportResult.Valid) >= sequencerTrustThreshold.unwrap)
      Right(())
    else {
      val errors = results
        .collect { case ValidateTransportResult.NotValid(message) => message }
      Left(errors.mkString(", "))
    }
  }

  def makeTransport(
      connection: SequencerConnection,
      member: Member,
      requestSigner: RequestSigner,
  )(implicit
      executionContext: ExecutionContextExecutor,
      executionSequencerFactory: ExecutionSequencerFactory,
      materializer: Materializer,
      traceContext: TraceContext,
  ): EitherT[Future, String, SequencerClientTransport]

  def validateTransport(
      connection: SequencerConnection,
      logWarning: Boolean,
  )(implicit
      executionContext: ExecutionContextExecutor,
      errorLoggingContext: ErrorLoggingContext,
      closeContext: CloseContext,
  ): EitherT[FutureUnlessShutdown, String, Unit]

}

object SequencerClientTransportFactory {
  sealed trait ValidateTransportResult extends Product with Serializable
  object ValidateTransportResult {
    final case object Valid extends ValidateTransportResult
    final case class NotValid(message: String) extends ValidateTransportResult
  }

  def validateTransport(
      connection: SequencerConnection,
      traceContextPropagation: TracingConfig.Propagation,
      config: SequencerClientConfig,
      logWarning: Boolean,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContextExecutor,
      errorLoggingContext: ErrorLoggingContext,
      closeContext: CloseContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] = connection match {
    case conn: GrpcSequencerConnection =>
      implicit val traceContext = errorLoggingContext.traceContext
      errorLoggingContext.logger.info(s"Validating sequencer connection ${conn}")
      val channelBuilder = ClientChannelBuilder(loggerFactory)
      val channel = GrpcSequencerChannelBuilder(
        channelBuilder,
        conn,
        NonNegativeInt.maxValue,
        traceContextPropagation,
        config.keepAliveClient,
      )
      def closeChannel(): Unit = {
        Lifecycle.close(
          Lifecycle.toCloseableChannel(
            channel,
            errorLoggingContext.logger,
            "sequencer-connection-test-channel",
          )
        )(
          errorLoggingContext.logger
        )
      }
      // clientConfig.handshakeRetryDelay.underlying.fromNow,
      val retryMs = config.initialConnectionRetryDelay.asFiniteApproximation
      val attempts = config.handshakeRetryDelay.underlying.toMillis / retryMs.toMillis
      def check(): EitherT[Future, String, Unit] = {
        channel.getState(true) match {
          case ConnectivityState.READY =>
            errorLoggingContext.logger.info(s"Successfully connected to sequencer at ${conn}")
            EitherT.rightT(())
          case other =>
            val msg = s"Unable to connect to sequencer at ${conn}: channel is ${other}"
            errorLoggingContext.debug(msg)
            EitherT.leftT(msg)
        }
      }
      val name = "check-valid-sequencer-connection"
      EitherT(
        retry
          .Pause(
            errorLoggingContext.logger,
            closeContext.context,
            maxRetries = attempts.toInt,
            delay = retryMs,
            operationName = name,
          )
          .unlessShutdown(
            closeContext.context.performUnlessClosingF(name)(check().value),
            NoExnRetryable,
          )
      ).thereafter { _ =>
        closeChannel()
      }.leftMap { res =>
        if (logWarning) {
          errorLoggingContext.logger.warn(res)
        }
        res
      }
  }
}
