// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import com.digitalasset.canton.lifecycle.UnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.Miscellaneous.objId
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.*
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.stub.StreamObserver

class P2PGrpcBftOrderingService(
    createServerSidePeerReceiver: StreamObserver[
      BftOrderingMessage
    ] => UnlessShutdown[StreamObserver[
      BftOrderingMessage
    ]],
    override val loggerFactory: NamedLoggerFactory,
) extends BftOrderingServiceGrpc.BftOrderingService
    with NamedLogging {

  override def receive(
      peerSender: StreamObserver[BftOrderingMessage]
  ): StreamObserver[BftOrderingMessage] =
    createServerSidePeerReceiver(peerSender) match {
      case UnlessShutdown.Outcome(peerReceiver) =>
        peerReceiver
      case UnlessShutdown.AbortedDueToShutdown =>
        // No receiver created means that we're shutting down
        implicit val traceContext: TraceContext = TraceContext.empty
        logger.debug(s"Completing peer sender ${objId(peerSender)} due to shutdown")
        peerSender.onCompleted()
        new StreamObserver[BftOrderingMessage]() {
          override def onNext(value: BftOrderingMessage): Unit =
            logger.debug(s"Received message $value, ignoring due to shutdown")
          override def onError(t: Throwable): Unit =
            logger.debug(s"Received error, ignoring due to shutdown", t)
          override def onCompleted(): Unit =
            logger.debug(s"Received completion")
        }
    }
}
