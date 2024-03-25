// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.server.pekko

import org.apache.pekko.stream.scaladsl.Sink
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.server.rs.ServerSubscriber
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import io.grpc.{StatusException, StatusRuntimeException}

import scala.concurrent.{Future, Promise}

object ServerAdapter {

  def toSink[Resp](
      streamObserver: StreamObserver[Resp],
      errorHandler: Throwable => Throwable,
  )(implicit executionSequencerFactory: ExecutionSequencerFactory): Sink[Resp, Future[Unit]] = {
    val subscriber =
      new ServerSubscriber[Resp](
        streamObserver.asInstanceOf[ServerCallStreamObserver[Resp]],
        executionSequencerFactory.getExecutionSequencer,
      ) {

        /** Translate unhandled exceptions arising inside Pekko streaming into self-service error codes.
          */
        override protected def translateThrowableInOnError(throwable: Throwable): Throwable = {
          throwable match {
            case t: StatusException => t
            case t: StatusRuntimeException => t
            case _ => errorHandler(throwable)
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
