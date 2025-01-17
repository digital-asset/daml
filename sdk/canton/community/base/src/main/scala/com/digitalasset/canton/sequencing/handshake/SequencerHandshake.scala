// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.handshake

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.sequencing.protocol.{HandshakeRequest, HandshakeResponse}
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.retry
import com.digitalasset.canton.util.retry.RetryUtil.AllExnRetryable
import com.digitalasset.canton.util.retry.Success
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

/** Performs the sequencer handshake. */
class SequencerHandshake(
    clientVersions: Seq[ProtocolVersion],
    minimumProtocolVersion: Option[ProtocolVersion],
    fetchHandshake: Traced[HandshakeRequest] => EitherT[
      Future,
      HandshakeRequestError,
      HandshakeResponse,
    ],
    retryAttempts: Int,
    retryDelay: FiniteDuration,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {
  def handshake: EitherT[Future, String, Unit] = withNewTraceContext { implicit traceContext =>
    logger.debug("Starting sequencer handshake")

    for {
      response <- repeatedlyTryToFetchHandshake
        .leftMap(err => s"Failed to fetch handshake response from sequencer server: $err")
      // determine whether the server accepted our handshake, log the outcome
      _ <- EitherT.fromEither[Future](
        response match {
          case HandshakeResponse.Success(serverVersion) =>
            logger.debug(
              s"Sequencer handshake successful (client v$clientVersions, server v$serverVersion)"
            )
            Right(())
          case HandshakeResponse.Failure(serverVersion, reason) =>
            logger.warn(
              s"Sequencer handshake failed: $reason (client versions: ${clientVersions
                  .mkString(", ")}, server: $serverVersion)"
            )
            Left(reason)
        }
      )
    } yield ()
  }

  private def repeatedlyTryToFetchHandshake(implicit
      traceContext: TraceContext
  ): EitherT[Future, HandshakeRequestError, HandshakeResponse] = {
    implicit val completed = Success[Either[HandshakeRequestError, HandshakeResponse]] {
      case Right(_) => true // we've got a response
      case Left(HandshakeRequestError(_, shouldRetry)) =>
        !shouldRetry // if we have a unretryable error we should finish immediately
    }
    val request = HandshakeRequest(clientVersions, minimumProtocolVersion)

    EitherT {
      retry
        .Pause(logger, this, maxRetries = retryAttempts, delay = retryDelay, "fetch handshake")
        .apply(
          {
            logger.trace("Attempting sequencer handshake")
            fetchHandshake(Traced(request)).value
          },
          AllExnRetryable,
        )
    }
  }
}

object SequencerHandshake {

  def handshake(
      supportedProtocols: Seq[ProtocolVersion],
      minimumProtocolVersion: Option[ProtocolVersion],
      client: SupportsHandshake,
      config: SequencerClientConfig,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): EitherT[Future, String, Unit] = {
    val sequencerHandshake =
      new SequencerHandshake(
        supportedProtocols,
        minimumProtocolVersion,
        Traced.lift(client.handshake(_)(_)),
        config.handshakeRetryAttempts.unwrap,
        config.handshakeRetryDelay.underlying,
        timeouts,
        loggerFactory,
      )

    sequencerHandshake.handshake.thereafter { _ =>
      sequencerHandshake.close()
    }
  }
}
