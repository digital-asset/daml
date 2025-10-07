// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.standalone

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.block.BlockFormat
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.{
  completeGrpcStreamObserver,
  failGrpcStreamObserver,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.Miscellaneous.mutex
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.standalone.v1.{
  Ordered as ProtoOrdered,
  ReadOrderedRequest,
  ReadOrderedResponse,
  SendRequest,
  SendResponse,
  StandaloneBftOrderingServiceGrpc,
}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.stub.StreamObserver

import scala.collection.mutable
import scala.concurrent.Future

class P2PGrpcStandaloneBftOrderingService(
    orderSendRequest: SendRequest => Future[SendResponse],
    override val loggerFactory: NamedLoggerFactory,
) extends StandaloneBftOrderingServiceGrpc.StandaloneBftOrderingService
    with NamedLogging
    with AutoCloseable {

  private val readers = mutable.ListBuffer[(Long, StreamObserver[ReadOrderedResponse])]()

  def push(block: BlockFormat.Block): Unit = {
    val failed = mutable.ListBuffer[StreamObserver[ReadOrderedResponse]]()
    mutex(this) {
      readers.foreach { case (minHeight, peerSender) =>
        if (block.blockHeight >= minHeight) {
          val response =
            ReadOrderedResponse(
              block.blockHeight,
              block.requests.map(r => ProtoOrdered(r.value.tag, r.value.body)),
            )
          try {
            peerSender.onNext(response)
          } catch {
            case e: Throwable =>
              logger.error(
                s"Failed to push block ${block.blockHeight} to reader $peerSender " +
                  s"with minHeight $minHeight: ${e.getMessage}",
                e,
              )(TraceContext.empty)
              failGrpcStreamObserver(peerSender, e, logger)(TraceContext.empty)
              failed.addOne(peerSender).discard
          }
        }
      }
    }
    mutex(this) {
      readers.filterInPlace { case (_, peerSender) =>
        !failed.contains(peerSender)
      }.discard
    }
  }

  override def send(request: SendRequest): Future[SendResponse] =
    orderSendRequest(request)

  override def readOrdered(
      request: ReadOrderedRequest,
      peerSender: StreamObserver[ReadOrderedResponse],
  ): Unit = mutex(this) {
    readers.addOne(request.startHeight -> peerSender).discard
  }

  override def close(): Unit =
    mutex(this) {
      readers.foreach { case (_, peerSender) =>
        completeGrpcStreamObserver(peerSender, logger)(TraceContext.empty)
      }
      readers.clear()
    }
}
