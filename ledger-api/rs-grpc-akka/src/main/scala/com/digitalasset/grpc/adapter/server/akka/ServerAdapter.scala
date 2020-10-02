// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.server.akka

import akka.stream.scaladsl.Sink
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.server.rs.ServerSubscriber
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}

import scala.concurrent.{Future, Promise}

object ServerAdapter {

  def toSink[Resp](streamObserver: StreamObserver[Resp])(
      implicit executionSequencerFactory: ExecutionSequencerFactory): Sink[Resp, Future[Unit]] = {
    val subscriber =
      new ServerSubscriber[Resp](
        streamObserver.asInstanceOf[ServerCallStreamObserver[Resp]],
        executionSequencerFactory.getExecutionSequencer)
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
