// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.grpc

import akka.NotUsed
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.{KillSwitch, KillSwitches, Materializer}
import com.daml.error.ContextualizedErrorLogger
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.server.akka.ServerAdapter
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.ledger.error.CommonErrors
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.StatusRuntimeException
import io.grpc.stub.StreamObserver

import scala.collection.concurrent.TrieMap
import scala.concurrent.blocking

trait StreamingServiceLifecycleManagement extends AutoCloseable with NamedLogging {

  private val directEc = DirectExecutionContext(noTracingLogger)

  @volatile private var _closed = false
  private val _killSwitches = TrieMap.empty[KillSwitch, Object]

  def close(): Unit = blocking {
    synchronized {
      if (!_closed) {
        _closed = true
        _killSwitches.keySet.foreach(_.abort(closingError(errorLoggingContext(TraceContext.empty))))
        _killSwitches.clear()
      }
    }
  }

  private def errorHandler(throwable: Throwable): StatusRuntimeException =
    CommonErrors.ServiceInternalError
      .UnexpectedOrUnknownException(throwable)(errorLoggingContext(TraceContext.empty))
      .asGrpcError

  protected def registerStream[RespT](
      responseObserver: StreamObserver[RespT]
  )(createSource: => Source[RespT, NotUsed])(implicit
      materializer: Materializer,
      executionSequencerFactory: ExecutionSequencerFactory,
  ): Unit = {
    def ifNotClosed(run: () => Unit): Unit =
      if (_closed) responseObserver.onError(closingError(errorLoggingContext(TraceContext.empty)))
      else run()

    // Double-checked locking to keep the (potentially expensive)
    // by-name `source` evaluation out of the synchronized block
    ifNotClosed { () =>
      val sink = ServerAdapter.toSink(responseObserver, errorHandler)
      // Force evaluation before synchronized block
      val source = createSource

      blocking {
        synchronized {
          ifNotClosed { () =>
            val (killSwitch, doneF) = source
              .viaMat(KillSwitches.single)(Keep.right)
              .watchTermination()(Keep.both)
              .toMat(sink)(Keep.left)
              .run()

            _killSwitches += killSwitch -> NotUsed

            // This can complete outside the synchronized block
            // maintaining the need of using a concurrent collection for _killSwitches
            doneF.onComplete(_ => _killSwitches -= killSwitch)(directEc)
          }
        }
      }
    }
  }

  private def closingError(errorLogger: ContextualizedErrorLogger): StatusRuntimeException =
    CommonErrors.ServerIsShuttingDown.Reject()(errorLogger).asGrpcError
}
