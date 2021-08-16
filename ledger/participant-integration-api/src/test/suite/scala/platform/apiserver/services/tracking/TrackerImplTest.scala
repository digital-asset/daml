// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.tracking

import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Keep, Source, SourceQueueWithComplete}
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.{Done, NotUsed}
import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.testing.utils.{AkkaBeforeAndAfterAll, TestingException}
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.commands.Commands
import com.daml.ledger.client.services.commands.tracker.CompletionResponse
import com.daml.ledger.client.services.commands.tracker.CompletionResponse.QueueSubmitFailure
import com.daml.logging.LoggingContext
import com.google.rpc.status.{Status => StatusProto}
import io.grpc.Status.Code
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{BeforeAndAfterEach, Inside, Succeeded}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class TrackerImplTest
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterEach
    with ScalaFutures
    with AkkaBeforeAndAfterAll
    with Inside {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(5.seconds, 1.second)

  private var sut: Tracker = _
  private var consumer: TestSubscriber.Probe[NotUsed] = _
  private var queue: SourceQueueWithComplete[TrackerImpl.QueueInput] = _
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private def input(cid: Int) = SubmitAndWaitRequest(Some(Commands(commandId = cid.toString)))

  override protected def beforeEach(): Unit = {
    val (q, sink) = Source
      .queue[TrackerImpl.QueueInput](1, OverflowStrategy.dropNew)
      .map { in =>
        in.context.success(
          Right(
            CompletionResponse.CompletionSuccess(in.value.getCommands.commandId, "", StatusProto())
          )
        )
        NotUsed
      }
      .toMat(TestSink.probe[NotUsed])(Keep.both)
      .run()
    queue = q
    sut = new TrackerImpl(q, Future.successful(Done))
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
        whenReady(sut.track(input(2)))(failure => {
          inside(failure) { case Left(QueueSubmitFailure(statusCode)) =>
            statusCode.getCode should be(Code.RESOURCE_EXHAUSTED)
          }
        })
      }
    }

    "input is submitted, and the queue has been completed" should {

      "return an ABORTED error" in {

        queue.complete()
        whenReady(sut.track(input(2)))(failure => {
          inside(failure) { case Left(QueueSubmitFailure(statusCode)) =>
            statusCode.getCode should be(Code.ABORTED)
          }
        })
      }
    }

    "input is submitted, and the queue has failed" should {

      "return an ABORTED error" in {

        queue.fail(TestingException("The queue fails with this error."))
        whenReady(sut.track(input(2)))(failure => {
          inside(failure) { case Left(QueueSubmitFailure(statusCode)) =>
            statusCode.getCode should be(Code.ABORTED)
          }
        })
      }
    }
  }
}
