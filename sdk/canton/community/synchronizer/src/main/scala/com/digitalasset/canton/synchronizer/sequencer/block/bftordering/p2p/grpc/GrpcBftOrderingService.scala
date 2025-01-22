// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.p2p.grpc

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v1.{
  BftOrderingServiceGrpc,
  BftOrderingServiceReceiveRequest,
  BftOrderingServiceReceiveResponse,
  PingRequest,
  PingResponse,
}
import io.grpc.stub.StreamObserver

import scala.concurrent.Future

class GrpcBftOrderingService(
    tryCreateServerEndpoint: StreamObserver[BftOrderingServiceReceiveResponse] => StreamObserver[
      BftOrderingServiceReceiveRequest
    ],
    override val loggerFactory: NamedLoggerFactory,
) extends BftOrderingServiceGrpc.BftOrderingService
    with NamedLogging {

  override def ping(request: PingRequest): Future[PingResponse] =
    Future.successful(PingResponse.defaultInstance)

  override def receive(
      clientEndpoint: StreamObserver[BftOrderingServiceReceiveResponse]
  ): StreamObserver[BftOrderingServiceReceiveRequest] =
    tryCreateServerEndpoint(clientEndpoint)
}
