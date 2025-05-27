// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.p2p.grpc

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingServiceReceiveResponse
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.stub.StreamObserver

import java.time.{Duration, Instant}

final class GrpcServerHandle[NetworkMessage](
    inputModule: ModuleRef[NetworkMessage],
    clientHandle: StreamObserver[BftOrderingServiceReceiveResponse],
    cleanupClientHandle: StreamObserver[BftOrderingServiceReceiveResponse] => Unit,
    getMessageSendInstant: NetworkMessage => Option[Instant],
    override val loggerFactory: NamedLoggerFactory,
    metrics: BftOrderingMetrics,
)(implicit metricsContext: MetricsContext)
    extends StreamObserver[NetworkMessage]
    with NamedLogging {

  override def onNext(message: NetworkMessage): Unit = {
    getMessageSendInstant(message).foreach(sendInstant =>
      metrics.p2p.send.grpcLatency.update(Duration.between(sendInstant, Instant.now))
    )
    inputModule.asyncSend(message)
  }

  override def onError(t: Throwable): Unit = {
    logger.info(
      s"a client errored (${t.getMessage}): connection severed, cleaning up client handle",
      t,
    )(TraceContext.empty)
    cleanupClientHandle(clientHandle)
  }

  override def onCompleted(): Unit = {
    logger.info(
      s"a client completed the stream, cleaning up client handle"
    )(TraceContext.empty)
    cleanupClientHandle(clientHandle)
  }
}
