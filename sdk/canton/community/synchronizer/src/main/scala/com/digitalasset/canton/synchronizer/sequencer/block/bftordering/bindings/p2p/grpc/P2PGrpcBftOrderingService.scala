// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.*
import io.grpc.stub.StreamObserver

class P2PGrpcBftOrderingService(
    tryCreateServerSidePeerReceiver: StreamObserver[
      BftOrderingMessage
    ] => StreamObserver[
      BftOrderingMessage
    ],
    override val loggerFactory: NamedLoggerFactory,
) extends BftOrderingServiceGrpc.BftOrderingService
    with NamedLogging {

  override def receive(
      peerSender: StreamObserver[BftOrderingMessage]
  ): StreamObserver[BftOrderingMessage] =
    tryCreateServerSidePeerReceiver(peerSender)
}
