// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.server.akka

import akka.NotUsed
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.{KillSwitch, KillSwitches, Materializer}
import com.daml.grpc.adapter.ExecutionSequencerFactory
import io.grpc.stub.StreamObserver
import scalapb.GeneratedMessage

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

trait StreamingServiceLifecycleManagement extends AutoCloseable {
  @volatile private var _closed = false
  private val _killSwitches = TrieMap.empty[KillSwitch, Object]
  protected def optimizeGrpcStreamsThroughput: Boolean

  def close(): Unit = synchronized {
    if (!_closed) {
      _closed = true
      _killSwitches.keySet.foreach(_.abort(ServerAdapter.closingError()))
      _killSwitches.clear()
    }
  }

  protected def registerStream[RespT <: GeneratedMessage with AnyRef](
      buildSource: () => Source[RespT, NotUsed],
      responseObserver: StreamObserver[RespT],
  )(implicit
      materializer: Materializer,
      executionSequencerFactory: ExecutionSequencerFactory,
  ): Unit = {
    def ifNotClosed(run: () => Unit): Unit =
      if (_closed) responseObserver.onError(ServerAdapter.closingError())
      else run()

    ifNotClosed { () =>
      val sink = ServerAdapter.toSink(responseObserver)
      val source = buildSource()
      val sourceWithPotentialOptimization =
        if (optimizeGrpcStreamsThroughput) source.precomputeSerializedSize
        else source

      // Double-checked locking to keep the (potentially expensive)
      // buildSource() step out of the synchronized block
      synchronized {
        ifNotClosed { () =>
          val (killSwitch, doneF) =
            sourceWithPotentialOptimization
              .viaMat(KillSwitches.single)(Keep.right)
              .watchTermination()(Keep.both)
              .toMat(sink)(Keep.left)
              .run()

          _killSwitches += killSwitch -> NotUsed

          // This can complete outside the synchronized block
          // maintaining the need of using a concurrent collection for _killSwitches
          doneF.onComplete(_ => _killSwitches -= killSwitch)(ExecutionContext.parasitic)
        }
      }
    }
  }

  implicit class ScalaPbOptimizationsFlow[ScalaPbMessage <: GeneratedMessage with AnyRef, Mat](
      val original: Source[ScalaPbMessage, Mat]
  ) {

    /** Optimization for gRPC stream throughput.
      *
      * gRPC internal logic marshalls the protobuf response payloads sequentially before
      * sending them over the wire (see io.grpc.ServerCallImpl.sendMessageInternal), imposing as limit
      * the maximum marshalling throughput of a payload type.
      *
      * We've observed empirically that ScalaPB-generated message classes have associated marshallers
      * with significant latencies when encoding complex payloads (e.g. [[com.daml.ledger.api.v1.transaction_service.GetTransactionTreesResponse]]),
      * with the gRPC marshalling bottleneck appearing in some performance tests.
      *
      * As an alleviation of the problem, we can leverage the fact that ScalaPB message classes have the serializedSize value memoized,
      * (see [[scalapb.GeneratedMessage.writeTo]]), whose computation is roughly half of the entire marshalling step.
      *
      * This optimization method takes advantage of the memoized value and forces the message's serializedSize computation,
      * roughly doubling the maximum theoretical ScalaPB stream throughput over the gRPC server layer.
      *
      * @return A new source with precomputed serializedSize for the [[scalapb.GeneratedMessage]]
      */
    def precomputeSerializedSize: Source[ScalaPbMessage, Mat] =
      original.map { msg =>
        // At ScalaPB 0.11.8, computation of serializedSize is thread-safe but the memoization mechanism
        // which this optimization is relying on is not.
        // Hence, synchronize on the message to ensure across-threads visibility of the memoized value,
        // with minimal performance impact since the monitor is un-contended.
        msg.synchronized {
          val _ = msg.serializedSize
        }
        msg
      }.async
  }
}
