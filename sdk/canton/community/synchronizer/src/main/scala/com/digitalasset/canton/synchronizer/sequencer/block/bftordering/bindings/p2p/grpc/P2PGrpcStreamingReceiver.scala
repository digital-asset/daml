// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.PromiseUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics.updateTimer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.topology.SequencerNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcStreamingReceiver.AuthenticationTimeout
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.Miscellaneous.{
  abort,
  objIdC,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingMessage
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.DelayUtil
import io.grpc.stub.StreamObserver

import java.time.{Duration, Instant}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Failure

abstract class P2PGrpcStreamingReceiver(
    maybeP2PEndpointId: Option[P2PEndpoint.Id],
    inputModule: ModuleRef[BftOrderingMessage],
    sequencerIdPromiseUS: PromiseUnlessShutdown[SequencerId],
    isAuthenticationEnabled: Boolean,
    metrics: BftOrderingMetrics,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext, metricsContext: MetricsContext)
    extends StreamObserver[BftOrderingMessage]
    with NamedLogging
    with AutoCloseable {

  override def toString: String = objIdC(this)

  def shutdown(): Unit

  private implicit val traceContext: TraceContext = TraceContext.empty

  private[grpc] val remotePeerId: String =
    maybeP2PEndpointId
      .map(_.toString)
      .getOrElse(s"<unknown (incoming connection, receiver: $this)>")

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  @volatile private var closed = false

  setupAuthenticationTimeout()

  override final def close(): Unit = {
    closed = true
    shutdown()
  }

  final override def onNext(message: BftOrderingMessage): Unit = {
    implicit val traceContext: TraceContext = TraceContext.fromW3CTraceParent(message.traceContext)
    logger.trace(s"$this: Received gRPC message from '$remotePeerId': $message")
    message.sentAt.foreach(sendInstant =>
      updateTimer(
        metrics.p2p.send.grpcLatency,
        Duration.between(sendInstant.asJavaInstant, Instant.now),
      )
    )
    if (!sequencerIdPromiseUS.isCompleted) {
      if (isAuthenticationEnabled) {
        val errorMsg =
          s"Received gRPC message before authentication was completed"
        sequencerIdPromiseUS.complete(Failure(new RuntimeException(errorMsg)))
        abort(logger, errorMsg)
      } else {
        logger.debug(
          s"Authentication disabled: received first gRPC message from '$remotePeerId'"
        )
        // Authentication is disabled: backfill the sequencer ID promise with the first message's sentBy
        SequencerId.fromProtoPrimitive(message.sentBy, "sentBy") match {
          case Left(e) =>
            val msg = s"$this: Received unparseable sequencer ID from '$remotePeerId': ${e.message}"
            logger.warn(msg)
            val error = new RuntimeException(msg)
            sequencerIdPromiseUS.complete(Failure(error))
            onError(error)
          case Right(sequencerId) =>
            logger.debug(
              s"Providing the sequencer ID ${sequencerId.toProtoPrimitive} " +
                s"in 'sentBy' of in first gRPC message from '$remotePeerId'"
            )
            sequencerIdPromiseUS.outcome_(sequencerId)
        }
      }
    } else {
      logger.trace(
        s"$this: Received sequencer ID for '$remotePeerId' already from authentication or first message"
      )
    }

    sequencerIdPromiseUS.futureUS
      .map { sequencerId =>
        if (validateNodeId(message, sequencerId)) {
          logger.trace(
            s"Forwarding gRPC message from '$remotePeerId' to p2p network input module: $message"
          )
          inputModule.asyncSend(message)
        }
      }
      .onShutdown(
        logger.debug(s"Dropping gRPC message from '$remotePeerId' on shutdown")
      )
      .discard
  }

  private def validateNodeId(message: BftOrderingMessage, sequencerId: SequencerId): Boolean = {
    def emitNonCompliance(
        metrics: BftOrderingMetrics
    )(from: BftNodeId)(implicit mc: MetricsContext): Unit = {
      val mcWithLabels = mc.withExtraLabels(
        metrics.security.noncompliant.labels.Sequencer -> from,
        metrics.security.noncompliant.labels.violationType.Key ->
          metrics.security.noncompliant.labels.violationType.values.WrongGrpcMessageSentByBftNodeId,
      )
      metrics.security.noncompliant.behavior.mark()(mcWithLabels)
    }

    val bftNodeId = SequencerNodeId.toBftNodeId(sequencerId)

    if (bftNodeId != message.sentBy) {
      // Signature verification uses the `from` field (a.k.a. original sender) from the underlying (signed) message,
      //  so validating the `sentBy` field from the top-level message is fine.
      val msg =
        s"$this: Sequencer ID (`$sequencerId`) from authentication and `sentBy` (`${message.sentBy}`) " +
          s"do not match in gRPC message from '$remotePeerId'"
      logger.warn(msg)
      emitNonCompliance(metrics)(bftNodeId)
      onError(new RuntimeException(msg))
      false
    } else true
  }

  final override def onError(t: Throwable): Unit = {
    closed = true
    logger.info(
      s"$this: Received error (${t.getMessage}) from '$remotePeerId', " +
        "invalidating connection and shutting down the gRPC channel",
      t,
    )
    close()
  }

  final override def onCompleted(): Unit = {
    closed = true
    logger.info(
      s"$this: Received completion from '$remotePeerId', " +
        "invalidating connection and shutting down the gRPC channel"
    )
    close()
  }

  // Don't wait forever for the sequencer ID to be provided:
  //
  // - If authentication is disabled, it comes from the first message but the peer could misbehave and not send it.
  // - Even if authentication is enabled, during shutdown the Canton member authenticator could be closed
  //   before we have a chance to obtain the sequencer ID, so if we don't complete the promise, we
  //   could potentially prevent the shutdown.
  private def setupAuthenticationTimeout(): Unit =
    DelayUtil.delay(AuthenticationTimeout).onComplete { _ =>
      if (!sequencerIdPromiseUS.isCompleted) {
        val msg =
          s"$this: Did not receive sequencer ID from '$remotePeerId' within $AuthenticationTimeout"
        val error = new RuntimeException(msg)
        sequencerIdPromiseUS.complete(Failure(error))
        if (!closed) {
          logger.info(msg + ", terminating the gRPC stream")
          onError(error)
        }
      } else {
        logger.debug(
          s"$this: Received sequencer ID from '$remotePeerId' before the authentication timeout"
        )
      }
    }
}

object P2PGrpcStreamingReceiver {

  // The timeout for receiving the sequencer ID from the server, after which
  //  the promise is completed with an error.
  //  It must be short enough to not block the shutdown of the node for too long,
  //  but long enough to allow the authentication to complete in a reasonable time.
  private val AuthenticationTimeout: FiniteDuration = 5.seconds
}
