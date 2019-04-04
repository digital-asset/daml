// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.grpc.adapter.utils.implementations

import java.util.concurrent.atomic.AtomicInteger

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Source}
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.grpc.adapter.server.akka.ServerAdapter
import com.digitalasset.grpc.sampleservice.Responding
import com.digitalasset.platform.hello.HelloServiceGrpc.HelloService
import com.digitalasset.platform.hello.{HelloRequest, HelloResponse, HelloServiceGrpc}
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver
import io.grpc.{BindableService, ServerServiceDefinition}

import scala.concurrent.ExecutionContext.Implicits.global

class AkkaImplementation(
    implicit executionSequencerFactory: ExecutionSequencerFactory,
    actorMaterializer: ActorMaterializer)
    extends HelloService
    with Responding
    with BindableService {

  private val serverStreamingCalls = new AtomicInteger()

  def getServerStreamingCalls: Int = serverStreamingCalls.get()

  override def bindService(): ServerServiceDefinition =
    HelloServiceGrpc.bindService(this, global)

  override def serverStreaming(
      request: HelloRequest,
      responseObserver: StreamObserver[HelloResponse]): Unit = {
    Source
      .single(request)
      .via(Flow[HelloRequest].mapConcat(responses))
      .runWith(ServerAdapter.toSink(responseObserver))
      .onComplete(_ => serverStreamingCalls.incrementAndGet())
  }

  private val clientStreamingFlow = {
    Flow[HelloRequest]
      .fold((0, ByteString.EMPTY))(
        (acc, request) =>
          (
            acc._1 + request.reqInt,
            ByteString.copyFrom(
              acc._2.toByteArray
                .zip(request.payload.toByteArray)
                .map(t => (t._1 ^ t._2).byteValue())))
      )
      .map(t => HelloResponse(t._1, t._2))
  }
}
