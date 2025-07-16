// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import com.digitalasset.canton.lifecycle.PromiseUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcStreamingClientSideReceiver.AuthenticationTimeout
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.Miscellaneous.abort
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingServiceReceiveResponse
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.DelayUtil
import io.grpc.stub.StreamObserver

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Failure

final class P2PGrpcStreamingClientSideReceiver(
    peerEndpoint: P2PEndpoint,
    sequencerIdPromiseUS: PromiseUnlessShutdown[SequencerId],
    isAuthenticationEnabled: Boolean,
    cleanupClientConnectionToServer: P2PEndpoint => Unit,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends StreamObserver[BftOrderingServiceReceiveResponse]
    with NamedLogging {

  private implicit val traceContext: TraceContext = TraceContext.empty

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  @volatile private var closed = false

  setupAuthenticationTimeout()

  override def onNext(response: BftOrderingServiceReceiveResponse): Unit = {
    logger.trace(s"$this: Received gRPC message from '${peerEndpoint.id}': $response")
    if (!sequencerIdPromiseUS.isCompleted) {
      if (isAuthenticationEnabled) {
        val errorMsg =
          s"Received gRPC message before authentication was completed"
        sequencerIdPromiseUS.complete(Failure(new RuntimeException(errorMsg)))
        abort(logger, errorMsg)
      } else {
        logger.debug(s"Received first gRPC message from '${peerEndpoint.id}'")
        // Authentication is disabled: backfill the sequencer ID promise with the first message's sentBy
        SequencerId.fromProtoPrimitive(response.from, "from") match {
          case Left(e) =>
            val msg =
              s"$this: Received unparseable sequencer ID from '${peerEndpoint.id}': $e"
            logger.warn(msg)
            val error = new RuntimeException(msg)
            sequencerIdPromiseUS.complete(Failure(error))
            onError(error)
          case Right(sequencerId) => sequencerIdPromiseUS.outcome_(sequencerId)
        }
      }
    } else {
      logger.trace(
        s"$this: Received sequencer ID for '${peerEndpoint.id}' already from authentication or first message," +
          s"ignoring subsequent message: $response"
      )
    }
  }

  override def onError(t: Throwable): Unit = {
    closed = true
    logger.info(
      s"$this: Received error (${t.getMessage}) from '${peerEndpoint.id}' in server role, " +
        "invalidating connection and shutting down the gRPC channel",
      t,
    )
    cleanupClientConnectionToServer(peerEndpoint)
  }

  override def onCompleted(): Unit = {
    closed = true
    logger.info(
      s"$this: Received completion from '${peerEndpoint.id}' in server role, " +
        "invalidating connection and shutting down the gRPC channel"
    )
    cleanupClientConnectionToServer(peerEndpoint)
  }

  // Don't wait forever for the sequencer ID to be provided:
  //
  // - If authentication is disabled, it comes from the first message but the peer could misbehave and not send it
  // - Even if authentication is enabled, during shutdown the Canton member authenticator could be closed
  //   before we have a chance to obtain the sequencer ID, so if we don't complete the promise, we
  //   could prevent the shutdown.
  private def setupAuthenticationTimeout(): Unit =
    DelayUtil.delay(AuthenticationTimeout).onComplete { _ =>
      if (!sequencerIdPromiseUS.isCompleted) {
        val msg =
          s"$this: Did not receive sequencer ID from '${peerEndpoint.id}' within $AuthenticationTimeout"
        val error = new RuntimeException(msg)
        sequencerIdPromiseUS.complete(Failure(error))
        if (!closed) {
          logger.info(msg + ", terminating the gRPC stream")
          onError(error)
        }
      } else {
        logger.debug(
          s"$this: Received sequencer ID from '${peerEndpoint.id}' before the authentication timeout"
        )
      }
    }
}

object P2PGrpcStreamingClientSideReceiver {

  // The timeout for receiving the sequencer ID from the server, after which
  //  the promise is completed with an error.
  //  It must be short enough to not block the shutdown of the node for too long,
  //  but long enough to allow the authentication to complete in a reasonable time.
  private val AuthenticationTimeout: FiniteDuration = 5.seconds
}
