// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.sampleservice.implementations

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Source}
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.sampleservice.HelloServiceResponding
import com.daml.platform.hello._
import io.grpc.{BindableService, ServerServiceDefinition, Status}

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext

class HelloServiceReferenceImplementation(
    protected implicit val esf: ExecutionSequencerFactory,
    protected implicit val mat: Materializer,
) extends HelloServiceAkkaGrpc
    with HelloServiceResponding
    with BindableService
    with AutoCloseable {

  private val serverStreamingCalls = new AtomicInteger()

  override def close(): Unit = ()

  override def bindService(): ServerServiceDefinition =
    HelloServiceGrpc.bindService(this, ExecutionContext.global)

  def getServerStreamingCalls: Int = serverStreamingCalls.get()

  private def validateRequest(request: HelloRequest): Unit =
    if (request.reqInt < 0)
      throw Status.INVALID_ARGUMENT
        .withDescription("request cannot be negative")
        .asRuntimeException()

  override protected def serverStreamingSource(
      request: HelloRequest
  ): Source[HelloResponse, NotUsed] = {
    validateRequest(request)
    Source
      .single(request)
      .via(Flow[HelloRequest].mapConcat(responses))
      .watchTermination() { (mat, doneF) =>
        doneF.onComplete(_ => serverStreamingCalls.incrementAndGet())(ExecutionContext.parasitic)
        mat
      }
  }
}
