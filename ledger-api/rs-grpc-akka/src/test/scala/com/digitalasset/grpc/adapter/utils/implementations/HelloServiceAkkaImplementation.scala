// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.utils.implementations

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Source}
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.sampleservice.HelloServiceResponding
import com.daml.platform.hello.HelloServiceGrpc.HelloService
import com.daml.platform.hello.{HelloRequest, HelloResponse, HelloServiceAkkaGrpc, HelloServiceGrpc}
import io.grpc.{BindableService, ServerServiceDefinition}

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext.Implicits.global

class HelloServiceAkkaImplementation(implicit
    val esf: ExecutionSequencerFactory,
    val mat: Materializer,
) extends HelloService
    with HelloServiceResponding
    with HelloServiceAkkaGrpc
    with BindableService {

  override protected def optimizeGrpcStreamsThroughput: Boolean = false
  private val serverStreamingCalls = new AtomicInteger()

  def getServerStreamingCalls: Int = serverStreamingCalls.get()

  override def bindService(): ServerServiceDefinition =
    HelloServiceGrpc.bindService(this, global)

  override protected def serverStreamingSource(
      request: HelloRequest
  ): Source[HelloResponse, NotUsed] =
    Source
      .single(request)
      .via(Flow[HelloRequest].mapConcat(responses))
      .watchTermination() { (mat, doneF) =>
        doneF.onComplete(_ => serverStreamingCalls.incrementAndGet())
        mat
      }
}
