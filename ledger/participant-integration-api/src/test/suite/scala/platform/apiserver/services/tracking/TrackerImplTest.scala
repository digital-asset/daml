// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.tracking

import akka.NotUsed
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Keep, Source, SourceQueueWithComplete}
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import com.daml.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  IsStatusException,
  TestingException
}
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.commands.Commands
import com.daml.ledger.api.v1.completion.Completion
import com.daml.dec.DirectExecutionContext
import com.google.rpc.status.{Status => RpcStatus}
import io.grpc.Status
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterEach, Matchers, Succeeded, WordSpec}

import scala.concurrent.ExecutionContext.Implicits.global

class TrackerImplTest
    extends WordSpec
    with Matchers
    with BeforeAndAfterEach
    with ScalaFutures
    with AkkaBeforeAndAfterAll {

  private var sut: Tracker = _
  private var consumer: TestSubscriber.Probe[NotUsed] = _
  private var queue: SourceQueueWithComplete[TrackerImpl.QueueInput] = _

  private def input(cid: Int) = SubmitAndWaitRequest(Some(Commands(commandId = cid.toString)))

  override protected def beforeEach(): Unit = {
    val (q, sink) = Source
      .queue[TrackerImpl.QueueInput](1, OverflowStrategy.dropNew)
      .map { in =>
        in.context.success(Completion(in.value.getCommands.commandId, Some(RpcStatus())))
        NotUsed
      }
      .toMat(TestSink.probe[NotUsed])(Keep.both)
      .run()
    queue = q
    sut = new TrackerImpl(q)
    consumer = sink
  }

  override protected def afterEach(): Unit = {
    consumer.cancel()
    queue.complete()
  }

  "Tracker Implementation" when {

    "input is submitted, and the queue is available" should {

      "work successfully" in {

        val resultF1 = sut.track(input(1))
        consumer.requestNext()
        val resultF = resultF1.flatMap(_ => sut.track(input(2)))(DirectExecutionContext)
        consumer.requestNext()
        whenReady(resultF)(_ => Succeeded)
      }
    }

    "input is submitted, and the queue is backpressuring" should {

      "return a RESOURCE_EXHAUSTED error" in {

        sut.track(input(1))
        whenReady(sut.track(input(2)).failed)(IsStatusException(Status.RESOURCE_EXHAUSTED))
      }
    }

    "input is submitted, and the queue has been completed" should {

      "return an ABORTED error" in {

        queue.complete()
        whenReady(sut.track(input(2)).failed)(IsStatusException(Status.ABORTED))
      }
    }

    "input is submitted, and the queue has failed" should {

      "return an ABORTED error" in {

        queue.fail(TestingException("The queue fails with this error."))
        whenReady(sut.track(input(2)).failed)(IsStatusException(Status.ABORTED))
      }
    }
  }
}
