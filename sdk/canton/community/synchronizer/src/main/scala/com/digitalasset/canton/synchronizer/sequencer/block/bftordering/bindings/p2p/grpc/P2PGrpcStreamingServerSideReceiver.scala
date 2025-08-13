// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics.updateTimer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingMessage
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.stub.StreamObserver

import java.time.{Duration, Instant}

final class P2PGrpcStreamingServerSideReceiver(
    inputModule: ModuleRef[BftOrderingMessage],
    peerSender: StreamObserver[BftOrderingMessage],
    cleanupPeerSender: StreamObserver[BftOrderingMessage] => Unit,
    override val loggerFactory: NamedLoggerFactory,
    metrics: BftOrderingMetrics,
)(implicit metricsContext: MetricsContext)
    extends StreamObserver[BftOrderingMessage]
    with NamedLogging {

  override def onNext(message: BftOrderingMessage): Unit = {
    message.sentAt.foreach(sendInstant =>
      updateTimer(
        metrics.p2p.send.grpcLatency,
        Duration.between(sendInstant.asJavaInstant, Instant.now),
      )
    )
    inputModule.asyncSendNoTrace(message)
  }

  override def onError(t: Throwable): Unit = {
    logger.info(
      s"a client errored (${t.getMessage}): connection severed, cleaning up client handle",
      t,
    )(TraceContext.empty)
    cleanupPeerSender(peerSender)
  }

  override def onCompleted(): Unit = {
    logger.info(
      s"a client completed the stream, cleaning up client handle"
    )(TraceContext.empty)
    cleanupPeerSender(peerSender)
  }
}
