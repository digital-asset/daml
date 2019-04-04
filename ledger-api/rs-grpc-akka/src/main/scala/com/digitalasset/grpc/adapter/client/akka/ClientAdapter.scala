// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.grpc.adapter.client.akka

import java.util.function.BiConsumer

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.grpc.adapter.client.rs.ClientPublisher
import io.grpc.stub.StreamObserver

object ClientAdapter {

  def serverStreaming[Req, Resp](req: Req, stub: (Req, StreamObserver[Resp]) => Unit)(
      implicit executionSequencerFactory: ExecutionSequencerFactory): Source[Resp, NotUsed] =
    Source.fromPublisher(
      new ClientPublisher[Req, Resp](req, adaptStub(stub), executionSequencerFactory))

  private def adaptStub[Req, Resp](
      stub: (Req, StreamObserver[Resp]) => Unit
  ): BiConsumer[Req, StreamObserver[Resp]] = { (req, resp) =>
    stub(req, resp)
  }

}
