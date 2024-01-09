// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.utils.implementations

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.server.pekko.ServerAdapter
import com.daml.grpc.sampleservice.HelloServiceResponding
import com.daml.platform.hello.HelloServiceGrpc.HelloService
import com.daml.platform.hello.{HelloRequest, HelloResponse, HelloServiceGrpc}
import io.grpc.stub.StreamObserver
import io.grpc.{BindableService, ServerServiceDefinition}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Flow, Source}

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext.Implicits.global

// TODO(#16172): Remove once we can use the artifact from Daml SDK
@SuppressWarnings(Array("com.digitalasset.canton.GlobalExecutionContext"))
class HelloServicePekkoImplementation(implicit
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
      .runWith(ServerAdapter.toSink(responseObserver, identity))
      .onComplete(_ => serverStreamingCalls.incrementAndGet())

}
