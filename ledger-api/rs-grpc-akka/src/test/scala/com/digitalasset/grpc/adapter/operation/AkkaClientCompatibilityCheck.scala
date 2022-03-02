// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.operation

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.stream.{Materializer, ThrottleMode}
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.client.ResultAssertions
import com.daml.grpc.adapter.client.akka.ClientAdapter
import com.daml.platform.hello.HelloRequest
import com.daml.platform.hello.HelloServiceGrpc.HelloServiceStub
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

trait AkkaClientCompatibilityCheck {
  self: AnyWordSpec with Matchers with ScalaFutures with ResultAssertions =>

  implicit protected def system: ActorSystem

  implicit protected def materializer: Materializer

  implicit protected def esf: ExecutionSequencerFactory

  def akkaClientCompatible(helloStub: => HelloServiceStub): Unit = {

    "respond with the correct number of elements and correct content in 1-* setup" in {
      val elemsF = ClientAdapter
        .serverStreaming(HelloRequest(elemCount), helloStub.serverStreaming)
        .runWith(Sink.seq)

      whenReady(elemsF)(assertElementsAreInOrder(elemCount.toLong))
    }

    "tolerate rematerialization of the same response source in 1-* setup" in {
      val source = ClientAdapter
        .serverStreaming(HelloRequest(elemCount), helloStub.serverStreaming)
      val elemsF1 = source.runWith(Sink.seq)
      val elemsF2 = source.runWith(Sink.seq)

      whenReady(for {
        elems1 <- elemsF1
        elems2 <- elemsF2
      } yield elems1 -> elems2)({ case (elems1, elems2) =>
        val check = assertElementsAreInOrder(elemCount.toLong) _
        check(elems1)
        check(elems2)
      })
    }

    "respond with the correct number of elements and correct content in 1-* setup when back-pressured" in {
      val elemsF = ClientAdapter
        .serverStreaming(HelloRequest(elemCount), helloStub.serverStreaming)
        .throttle(100, 1.second, 16, ThrottleMode.shaping)
        .runWith(Sink.seq)

      whenReady(elemsF)(assertElementsAreInOrder(elemCount.toLong))
    }

    "handle cancellation in 1-* setup" in {
      val elemsF = ClientAdapter
        .serverStreaming(HelloRequest(elemCount), helloStub.serverStreaming)
        .take(halfCount.toLong)
        .runWith(Sink.seq)

      whenReady(elemsF)(assertElementsAreInOrder(halfCount.toLong))
    }

  }
}
