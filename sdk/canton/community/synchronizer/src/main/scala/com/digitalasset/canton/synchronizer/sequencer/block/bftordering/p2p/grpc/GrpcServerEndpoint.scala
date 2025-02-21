// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.p2p.grpc

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingServiceReceiveResponse
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.stub.StreamObserver

final class GrpcServerEndpoint[NetworkMessage](
    inputModule: ModuleRef[NetworkMessage],
    clientEndpoint: StreamObserver[BftOrderingServiceReceiveResponse],
    cleanupClientEndpoint: StreamObserver[BftOrderingServiceReceiveResponse] => Unit,
    override val loggerFactory: NamedLoggerFactory,
) extends StreamObserver[NetworkMessage]
    with NamedLogging {

  override def onNext(value: NetworkMessage): Unit =
    inputModule.asyncSend(value)

  override def onError(t: Throwable): Unit = {
    logger.info(
      s"a client errored (${t.getMessage}), connection severed, cleaning up client endpoint"
    )(TraceContext.empty)
    cleanupClientEndpoint(clientEndpoint)
  }

  override def onCompleted(): Unit = {
    logger.info(
      s"a client completed the stream, cleaning up client endpoint"
    )(TraceContext.empty)
    cleanupClientEndpoint(clientEndpoint)
  }
}
