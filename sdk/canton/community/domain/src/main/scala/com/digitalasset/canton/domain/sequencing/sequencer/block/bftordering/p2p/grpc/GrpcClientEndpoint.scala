// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.p2p.grpc

import com.digitalasset.canton.domain.sequencing.sequencer.bftordering.v1.BftOrderingServiceReceiveResponse
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.topology.{SequencerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.DelayUtil
import io.grpc.stub.StreamObserver

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Promise}
import scala.util.{Failure, Success}

import GrpcClientEndpoint.AuthenticationTimeout

final class GrpcClientEndpoint(
    server: Endpoint,
    sequencerIdPromise: Promise[SequencerId],
    cleanupClientConnectionToServer: Endpoint => Unit,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends StreamObserver[BftOrderingServiceReceiveResponse]
    with NamedLogging {

  private implicit val traceContext: TraceContext = TraceContext.empty

  DelayUtil.delay(AuthenticationTimeout).onComplete { _ =>
    if (!sequencerIdPromise.isCompleted) {
      val msg =
        s"client role did not receive initial gRPC message from peer $server in server role within $AuthenticationTimeout"
      logger.error(msg)
      val error = new RuntimeException(msg)
      sequencerIdPromise.complete(Failure(error))
      onError(error)
    }
  }

  override def onNext(response: BftOrderingServiceReceiveResponse): Unit = {
    logger.debug(s"in client role received initial gRPC message from peer $server in server role")
    if (!sequencerIdPromise.isCompleted) {
      UniqueIdentifier.fromProtoPrimitive(response.sequencerUid, "sequencer_uid") match {
        case Left(e) =>
          val msg = s"received unparseable sequencer ID from peer $server in server role: $e"
          logger.error(msg)
          val error = new RuntimeException(msg)
          sequencerIdPromise.complete(Failure(error))
          onError(error)
        case Right(uid) => sequencerIdPromise.complete(Success(SequencerId(uid)))
      }
    } else {
      logger.error(
        s"in client role received further gRPC messages from peer $server in server role"
      )
    }
  }

  override def onError(t: Throwable): Unit = {
    logger.info(
      s"in client role received error (${t.getMessage}) from peer $server in server role, invalidating connection and shutting down the gRPC channel"
    )
    cleanupClientConnectionToServer(server)
  }

  override def onCompleted(): Unit = {
    logger.info(
      s"in client role received completion from peer $server in server role, invalidating connection and shutting down the gRPC channel"
    )
    cleanupClientConnectionToServer(server)
  }
}

object GrpcClientEndpoint {
  private val AuthenticationTimeout: FiniteDuration = 5.seconds
}
