// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.server.akka

import akka.stream.scaladsl.Sink
import com.daml.error.DamlContextualizedErrorLogger
import com.daml.error.definitions.LedgerApiErrors
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.server.rs.ServerSubscriber
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import io.grpc.{StatusException, StatusRuntimeException}
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}

import scala.concurrent.{Future, Promise}

object ServerAdapter {

  private val logger = ContextualizedLogger.get(getClass)
  private val emptyLoggingContext = LoggingContext.newLoggingContext(identity)

  def toSink[Resp](
      streamObserver: StreamObserver[Resp]
  )(implicit executionSequencerFactory: ExecutionSequencerFactory): Sink[Resp, Future[Unit]] = {
    val subscriber =
      new ServerSubscriber[Resp](
        streamObserver.asInstanceOf[ServerCallStreamObserver[Resp]],
        executionSequencerFactory.getExecutionSequencer,
      ) {

        /** Translate unhandled exceptions arising inside Akka streaming into self-service error codes.
          */
        override protected def translateThrowableInOnError(throwable: Throwable): Throwable = {
          throwable match {
            case t: StatusException => t
            case t: StatusRuntimeException => t
            case _ =>
              LedgerApiErrors.InternalError
                .UnexpectedOrUnknownException(throwable)(
                  new DamlContextualizedErrorLogger(logger, emptyLoggingContext, None)
                )
                .asGrpcError
          }
        }

      }

    Sink
      .fromSubscriber(subscriber)
      .mapMaterializedValue(_ => {
        val promise = Promise[Unit]()
        subscriber.completionFuture.handle[Unit]((_, throwable) => {
          if (throwable == null) promise.success(()) else promise.failure(throwable)
          ()
        })
        promise.future
      })
  }

}
