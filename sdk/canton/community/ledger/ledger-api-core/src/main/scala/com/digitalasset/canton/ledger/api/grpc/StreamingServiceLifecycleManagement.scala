// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.grpc

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.server.pekko.ServerAdapter
import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.ledger.error.CommonErrors
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.TryUtil
import io.grpc.StatusRuntimeException
import io.grpc.stub.StreamObserver
import org.apache.pekko.stream.scaladsl.{Keep, Source}
import org.apache.pekko.stream.{KillSwitch, KillSwitches, Materializer}
import org.apache.pekko.{Done, NotUsed}

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, blocking}

trait StreamingServiceLifecycleManagement extends AutoCloseable with NamedLogging {

  private val directEc = DirectExecutionContext(noTracingLogger)
  private val StreamAbortTimeout = Duration(10, "seconds")

  @volatile private var _closed = false
  private val _killSwitches = TrieMap.empty[KillSwitch, (TraceContext, Future[Done])]

  def close(): Unit = {
    implicit val ec = directEc
    val completions = blocking(synchronized {
      if (!_closed) {
        _closed = true
        val completionFs = _killSwitches.map { case (killSwitch, (traceContext, completeF)) =>
          killSwitch.abort(
            closingError(errorLoggingContext(traceContext))
          )
          completeF
        }
        _killSwitches.clear()
        completionFs
      } else Nil
    })
    // waiting for all the pekko-streams to finish
    discard(
      Await.result(
        awaitable = Future.sequence(
          completions.map(
            // we don't care about failures after abort
            _.transform(_ => TryUtil.unit)
          )
        ),
        atMost = StreamAbortTimeout,
      )
    )
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
      traceContext: TraceContext,
  ): Unit = {
    def ifNotClosed(run: () => Unit): Unit =
      if (_closed) responseObserver.onError(closingError(errorLoggingContext))
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

            logger.debug(s"Streaming to gRPC client started")

            _killSwitches += killSwitch -> (traceContext -> doneF)

            // This can complete outside the synchronized block
            // maintaining the need of using a concurrent collection for _killSwitches
            doneF.onComplete { _ =>
              logger.debug(s"Streaming to gRPC client finished")
              _killSwitches -= killSwitch
            }(directEc)
          }
        }
      }
    }
  }

  private def closingError(errorLogger: ErrorLoggingContext): StatusRuntimeException =
    GrpcErrors.AbortedDueToShutdown.Error()(errorLogger).asGrpcError
}
