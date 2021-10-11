// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.tracking

import akka.stream.scaladsl.{Keep, Source, SourceQueueWithComplete}
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.{Materializer, OverflowStrategy}
import akka.{Done, NotUsed}
import com.daml.grpc.GrpcStatus
import com.daml.ledger.api.testing.utils.{AkkaBeforeAndAfterAll, TestingException}
import com.daml.ledger.api.v1.commands.Commands
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.client.services.commands.CommandSubmission
import com.daml.ledger.client.services.commands.tracker.CompletionResponse
import com.daml.logging.LoggingContext
import com.daml.platform.apiserver.services.tracking.QueueBackedTracker.QueueInput
import com.daml.platform.apiserver.services.tracking.QueueBackedTrackerSpec._
import com.google.rpc.status.{Status => StatusProto}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{BeforeAndAfterEach, Inside}

import scala.concurrent.{ExecutionContext, Future}

class QueueBackedTrackerSpec
    extends AsyncWordSpec
    with Matchers
    with BeforeAndAfterEach
    with AkkaBeforeAndAfterAll
    with Inside {

  private implicit val ec: ExecutionContext = ExecutionContext.global
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private var consumer: TestSubscriber.Probe[NotUsed] = _
  private var queue: SourceQueueWithComplete[QueueBackedTracker.QueueInput] = _

  override protected def beforeEach(): Unit = {
    val (q, sink) = alwaysSuccessfulQueue(bufferSize = 1)
    queue = q
    consumer = sink
  }

  override protected def afterEach(): Unit = {
    consumer.cancel()
    queue.complete()
  }

  "Tracker Implementation" when {
    "input is submitted, and the queue is available" should {
      "work successfully" in {
        val tracker = new QueueBackedTracker(queue, Future.successful(Done))
        val completion1F = tracker.track(input(1))
        consumer.requestNext()
        val completion2F = tracker.track(input(2))
        consumer.requestNext()
        for {
          _ <- completion1F
          _ <- completion2F
        } yield succeed
      }
    }

    "input is submitted, and the queue is backpressuring" should {
      "return a RESOURCE_EXHAUSTED error" in {
        val tracker = new QueueBackedTracker(queue, Future.successful(Done))
        tracker.track(input(1))
        tracker.track(input(2)).map { completion =>
          completion should matchPattern {
            case Left(CompletionResponse.QueueSubmitFailure(GrpcStatus.RESOURCE_EXHAUSTED())) =>
          }
        }
      }
    }

    "input is submitted, and the queue has been completed" should {
      "return an ABORTED error" in {
        val tracker = new QueueBackedTracker(queue, Future.successful(Done))
        queue.complete()
        tracker.track(input(2)).map { completion =>
          completion should matchPattern {
            case Left(CompletionResponse.QueueSubmitFailure(GrpcStatus.ABORTED())) =>
          }
        }
      }
    }

    "input is submitted, and the queue has failed" should {
      "return an ABORTED error" in {
        val tracker = new QueueBackedTracker(queue, Future.successful(Done))
        queue.fail(TestingException("The queue fails with this error."))
        tracker.track(input(2)).map { completion =>
          completion should matchPattern {
            case Left(CompletionResponse.QueueSubmitFailure(GrpcStatus.ABORTED())) =>
          }
        }
      }
    }
  }
}

object QueueBackedTrackerSpec {
  private def input(commandId: Int) = CommandSubmission(Commands(commandId = commandId.toString))

  private def alwaysSuccessfulQueue(bufferSize: Int)(implicit
      materializer: Materializer
  ): (SourceQueueWithComplete[QueueInput], TestSubscriber.Probe[NotUsed]) =
    Source
      .queue[QueueInput](bufferSize, OverflowStrategy.dropNew)
      .map { in =>
        val completion = CompletionResponse.CompletionSuccess(
          Completion(
            commandId = in.value.commands.commandId,
            status = Some(StatusProto.defaultInstance),
            transactionId = "",
          )
        )
        in.context.success(Right(completion))
        NotUsed
      }
      .toMat(TestSink.probe[NotUsed](materializer.system))(Keep.both)
      .run()
}
