// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.utils.implementations

import java.util.concurrent.atomic.AtomicInteger

import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Source}
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.server.akka.ServerAdapter
import com.daml.grpc.sampleservice.HelloServiceResponding
import com.daml.platform.hello.HelloServiceGrpc.HelloService
import com.daml.platform.hello.{HelloRequest, HelloResponse, HelloServiceGrpc}
import io.grpc.stub.StreamObserver
import io.grpc.{BindableService, ServerServiceDefinition}

import scala.concurrent.ExecutionContext.Implicits.global

class HelloServiceAkkaImplementation(implicit
    executionSequencerFactory: ExecutionSequencerFactory,
    materializer: Materializer,
) extends HelloService
    with HelloServiceResponding
    with BindableService {

  private val serverStreamingCalls = new AtomicInteger()

  def getServerStreamingCalls: Int = serverStreamingCalls.get()

  override def bindService(): ServerServiceDefinition =
    HelloServiceGrpc.bindService(this, global)

  override def serverStreaming(
      request: HelloRequest,
      responseObserver: StreamObserver[HelloResponse],
  ): Unit =
    Source
      .single(request)
      .via(Flow[HelloRequest].mapConcat(responses))
      .runWith(ServerAdapter.toSink(responseObserver))
      .onComplete(_ => serverStreamingCalls.incrementAndGet())

}
