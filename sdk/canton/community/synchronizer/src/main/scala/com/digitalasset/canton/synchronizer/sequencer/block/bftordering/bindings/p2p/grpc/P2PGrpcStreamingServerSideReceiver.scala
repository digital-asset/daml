// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics.updateTimer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.{
  BftOrderingServiceReceiveRequest,
  BftOrderingServiceReceiveResponse,
}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.stub.StreamObserver

import java.time.{Duration, Instant}

final class P2PGrpcStreamingServerSideReceiver(
    inputModule: ModuleRef[BftOrderingServiceReceiveRequest],
    peerSender: StreamObserver[BftOrderingServiceReceiveResponse],
    cleanupPeerSender: StreamObserver[BftOrderingServiceReceiveResponse] => Unit,
    getMessageSendInstant: BftOrderingServiceReceiveRequest => Option[Instant],
    override val loggerFactory: NamedLoggerFactory,
    metrics: BftOrderingMetrics,
)(implicit metricsContext: MetricsContext)
    extends StreamObserver[BftOrderingServiceReceiveRequest]
    with NamedLogging {

  override def onNext(message: BftOrderingServiceReceiveRequest): Unit = {
    getMessageSendInstant(message).foreach(sendInstant =>
      updateTimer(metrics.p2p.send.grpcLatency, Duration.between(sendInstant, Instant.now))
    )
    inputModule.asyncSend(message)
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
