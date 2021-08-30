// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands

import java.time.{Duration, Instant}
import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Keep, Source, SourceQueueWithComplete}
import akka.stream.testkit.javadsl.TestSink
import akka.stream.testkit.scaladsl.TestSource
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import akka.stream.{OverflowStrategy, QueueOfferResult}
import com.daml.api.util.TimestampConversion._
import com.daml.concurrent.ExecutionContext
import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.api.v1.command_completion_service.Checkpoint
import com.daml.ledger.api.v1.commands.Commands
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset.LedgerBoundary.LEDGER_BEGIN
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset.Value.{Absolute, Boundary}
import com.daml.ledger.client.services.commands.tracker.CompletionResponse
import com.daml.ledger.client.services.commands.tracker.CompletionResponse.{
  CompletionFailure,
  CompletionSuccess,
  NotOkResponse,
}
import com.daml.util.Ctx
import com.google.protobuf.duration.{Duration => DurationProto}
import com.google.protobuf.empty.Empty
import com.google.protobuf.timestamp.Timestamp
import com.google.rpc.code._
import com.google.rpc.status.Status
import io.grpc.StatusRuntimeException
import org.scalatest.OptionValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration.DurationLong
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

class CommandTrackerFlowTest
    extends AsyncWordSpec
    with Matchers
    with OptionValues
    with AkkaBeforeAndAfterAll
    with ScalaFutures {

  type C[Value] = Ctx[(Int, String), Value]

  private val allSubmissionsSuccessful
      : Flow[Ctx[(Int, String), CommandSubmission], Ctx[(Int, String), Try[Empty]], NotUsed] =
    Flow[C[CommandSubmission]].map {
      _.map(_ => Success(Empty.defaultInstance))
    }

  private val shortDuration = Duration.ofSeconds(1L)

  private lazy val submissionSource = TestSource.probe[Ctx[Int, CommandSubmission]]
  private lazy val resultSink =
    TestSink.probe[Ctx[Int, Either[CompletionFailure, CompletionSuccess]]](system)

  private val mrt = Instant.EPOCH.plus(shortDuration)
  private val commandId = "commandId"
  private val abortedCompletion =
    Completion(
      commandId,
      Some(Status(Code.ABORTED.value)),
    )
  private val successStatus = Status(Code.OK.value)
  private val context = 1
  private val submission = newSubmission(commandId)
  private def newSubmission(commandId: String, dedupTime: Option[Duration] = None) = Ctx(
    context,
    CommandSubmission(
      Commands(
        commandId = commandId,
        deduplicationPeriod = dedupTime
          .map(t => Commands.DeduplicationPeriod.DeduplicationTime(DurationProto(t.getSeconds)))
          .getOrElse(Commands.DeduplicationPeriod.Empty),
      )
    ),
  )

  private case class Handle(
      submissions: TestPublisher.Probe[Ctx[Int, CommandSubmission]],
      completions: TestSubscriber.Probe[Ctx[Int, Either[CompletionFailure, CompletionSuccess]]],
      whatever: Future[Map[String, Int]],
      completionsStreamMock: CompletionStreamMock,
  )

  private class CompletionStreamMock() {

    case class State(
        queue: SourceQueueWithComplete[CompletionStreamElement],
        startOffset: LedgerOffset,
    )

    private implicit val ec: ExecutionContext[Nothing] = DirectExecutionContext
    private val stateRef = new AtomicReference[Promise[State]](Promise[State]())

    def createCompletionsSource(
        ledgerOffset: LedgerOffset
    ): Source[CompletionStreamElement, NotUsed] = {
      val (queue, completionSource) =
        Source
          .queue[CompletionStreamElement](Int.MaxValue, OverflowStrategy.backpressure)
          .preMaterialize()
      stateRef.get().success(State(queue, ledgerOffset))
      completionSource
    }

    def send(elem: CompletionStreamElement): Future[QueueOfferResult] =
      for {
        state <- stateRef.get().future
        res <- state.queue.offer(elem)
      } yield res

    def breakCompletionsStream(): Future[Unit] =
      stateRef
        .getAndSet(Promise[State]())
        .future
        .map(state => state.queue.fail(new RuntimeException("boom")))

    def getLastOffset: Future[LedgerOffset] =
      stateRef.get().future.map(_.startOffset)

  }

  "Command tracking flow" when {

    "two commands are submitted with the same ID" should {

      "fail the stream" in {

        val Handle(submissions, results, _, _) =
          runCommandTrackingFlow(allSubmissionsSuccessful)

        submissions.sendNext(submission)
        submissions.sendNext(submission)

        results.expectError() shouldBe a[RuntimeException]
      }
    }

    "the stream fails" should {

      "expose internal state as materialized value" in {

        val Handle(submissions, _, unhandledF, _) =
          runCommandTrackingFlow(allSubmissionsSuccessful)

        submissions.sendNext(submission)
        submissions.sendNext(submission)

        whenReady(unhandledF) { unhandled =>
          unhandled should have size 1
          unhandled should contain(
            submission.value.commands.commandId -> submission.context
          )
        }
      }
    }

    "the stream completes" should {

      "expose internal state as materialized value" in {

        val Handle(submissions, results, unhandledF, _) =
          runCommandTrackingFlow(allSubmissionsSuccessful)
        val otherCommandId = "otherId"

        submissions.sendNext(newSubmission(otherCommandId))

        results.cancel()
        whenReady(unhandledF) { unhandled =>
          unhandled should have size 1
          unhandled should contain(otherCommandId -> submission.context)
        }
      }
    }

    "submission input is closed" should {

      "complete the stage if no commands are tracked" in {
        val Handle(submissions, results, _, _) =
          runCommandTrackingFlow(allSubmissionsSuccessful)

        submissions.sendComplete()

        results.expectComplete()
        succeed
      }

      "keep the stage if there are tracked commands" in {
        val Handle(submissions, results, _, _) =
          runCommandTrackingFlow(allSubmissionsSuccessful)

        submissions.sendNext(submission)
        submissions.sendComplete()

        results.expectNoMessage(1.second)
        succeed
      }
    }

    "grpc error arrives for submission" should {

      "output it as a completion if terminal" in {

        val Handle(submissions, results, _, _) =
          runCommandTrackingFlow(Flow[C[CommandSubmission]].map {
            _.map(_ => Failure(new StatusRuntimeException(io.grpc.Status.RESOURCE_EXHAUSTED)))
          })

        submissions.sendNext(submission)

        val failureCompletion =
          Left(
            NotOkResponse(commandId = commandId, grpcStatus = Status(Code.RESOURCE_EXHAUSTED.value))
          )

        results.expectNext(Ctx(context, failureCompletion))
        succeed
      }

      "swallow error if not terminal" in {

        val Handle(submissions, results, _, completionStreamMock) =
          runCommandTrackingFlow(Flow[C[CommandSubmission]].map {
            _.map(_ => Failure(new StatusRuntimeException(io.grpc.Status.UNKNOWN)))
          })

        submissions.sendNext(submission)

        results.expectNoMessage(3.seconds)

        completionStreamMock.send(CompletionStreamElement.CompletionElement(abortedCompletion))
        results.requestNext().value shouldEqual Left(
          NotOkResponse(commandId, Status(Code.ABORTED.value))
        )
      }

      "swallow error if not terminal, then output completion when it arrives" in {

        val Handle(submissions, results, _, completionStreamMock) =
          runCommandTrackingFlow(Flow[C[CommandSubmission]].map {
            _.map(_ => Failure(new StatusRuntimeException(io.grpc.Status.UNKNOWN)))
          })

        submissions.sendNext(submission)

        completionStreamMock.send(CompletionStreamElement.CompletionElement(abortedCompletion))
        results.requestNext().value shouldEqual Left(
          NotOkResponse(commandId, Status(Code.ABORTED.value))
        )
      }

    }

    "no completion arrives" should {

      "not timeout the command while MRT <= RT" in {
        val Handle(submissions, results, _, completionStreamMock) =
          runCommandTrackingFlow(allSubmissionsSuccessful)

        submissions.sendNext(submission)

        completionStreamMock.send(
          CompletionStreamElement.CheckpointElement(Checkpoint(Some(fromInstant(mrt))))
        )

        results.expectNoMessage(1.second)
        succeed
      }

      "timeout the command when the MRT passes" in {
        val Handle(submissions, results, _, _) = runCommandTrackingFlow(allSubmissionsSuccessful)

        submissions.sendNext(newSubmission(commandId, dedupTime = Some(Duration.ofMillis(500))))

        results.expectNext(
          2.seconds,
          Ctx(context, Left(CompletionResponse.TimeoutResponse(commandId))),
        )
        succeed
      }

      "use the maximum expiry time, if provided" in {
        val Handle(submissions, results, _, _) = runCommandTrackingFlow(
          allSubmissionsSuccessful,
          maximumExpiryTime = Duration.ofSeconds(1),
        )

        submissions.sendNext(submission)

        results.expectNext(
          2.seconds,
          Ctx(context, Left(CompletionResponse.TimeoutResponse(commandId))),
        )
        succeed
      }

      "cap the timeout at the maximum expiry time" in {
        val Handle(submissions, results, _, _) = runCommandTrackingFlow(
          allSubmissionsSuccessful,
          maximumExpiryTime = Duration.ofSeconds(1),
        )

        submissions.sendNext(newSubmission(commandId, dedupTime = Some(Duration.ofSeconds(10))))

        results.expectNext(
          2.seconds,
          Ctx(context, Left(CompletionResponse.TimeoutResponse(commandId))),
        )
        succeed
      }
    }

    "successful completion arrives" should {

      "output the completion" in {
        val Handle(submissions, results, _, completionStreamMock) =
          runCommandTrackingFlow(allSubmissionsSuccessful)

        submissions.sendNext(submission)

        completionStreamMock.send(successfulCompletion(commandId))

        results.expectNext(
          Ctx(context, Right(CompletionResponse.CompletionSuccess(commandId, "", successStatus)))
        )
        succeed
      }

      "after the timeout" in {
        val Handle(submissions, results, _, completionStreamMock) =
          runCommandTrackingFlow(allSubmissionsSuccessful)
        val timedOutCommandId = "timedOutCommandId"
        val submitRequestShortDedupTime = newSubmission(timedOutCommandId, Some(shortDuration))
        submissions.sendNext(submitRequestShortDedupTime)

        results.expectNext(
          shortDuration.getSeconds.seconds * 3,
          Ctx(
            context,
            Left(
              CompletionResponse.TimeoutResponse(
                timedOutCommandId
              )
            ),
          ),
        )

        // since the command timed out before, the tracker shouldn't send the completion through
        completionStreamMock.send(successfulCompletion(timedOutCommandId))
        results.request(1)
        results.expectNoMessage()
        succeed
      }

      "after another command has timed out" in {
        val Handle(submissions, results, _, completionStreamMock) =
          runCommandTrackingFlow(allSubmissionsSuccessful)
        val timedOutCommandId = "timedOutCommandId"
        val submitRequestShortDedupTime = newSubmission(timedOutCommandId, Some(shortDuration))

        // we send 2 requests
        submissions.sendNext(submitRequestShortDedupTime)
        submissions.sendNext(submission)

        // the tracker observes the timeout before the completion, thus "consuming" the pull on the result output
        results.expectNext(
          3.seconds,
          Ctx(
            context,
            Left(
              CompletionResponse.TimeoutResponse(
                timedOutCommandId
              )
            ),
          ),
        )
        // we now receive a completion
        completionStreamMock.send(successfulCompletion(commandId))
        // because the out-of-band timeout completion consumed the previous pull on `results`,
        // we don't expect a message until we request one.
        // The order below is important to reproduce the issue described in DPP-285.
        results.expectNoMessage()
        results.request(1)
        results.expectNext(
          Ctx(context, Right(CompletionResponse.CompletionSuccess(commandId, "", successStatus)))
        )
        succeed
      }

    }

    "duplicate completion arrives for a particular command" should {

      "output the completion only once" in {

        val Handle(submissions, results, _, completionStreamMock) =
          runCommandTrackingFlow(allSubmissionsSuccessful)

        submissions.sendNext(submission)

        completionStreamMock.send(successfulCompletion(commandId))
        completionStreamMock.send(successfulCompletion(commandId))

        results.expectNext(
          Ctx(context, Right(CompletionResponse.CompletionSuccess(commandId, "", successStatus)))
        )
        results.expectNoMessage(1.second)
        succeed
      }
    }

    "failed completion arrives" should {

      "output the completion" in {

        val Handle(submissions, results, _, completionStreamMock) =
          runCommandTrackingFlow(allSubmissionsSuccessful)

        submissions.sendNext(submission)

        val status = Status(Code.INVALID_ARGUMENT.value)
        val failureCompletion =
          Completion(
            commandId,
            Some(status),
          )
        completionStreamMock.send(CompletionStreamElement.CompletionElement(failureCompletion))

        results.expectNext(
          Ctx(
            context,
            Left(CompletionResponse.NotOkResponse(commandId = commandId, grpcStatus = status)),
          )
        )
        succeed
      }
    }

    "a multitude of successful completions arrive for submitted commands" should {

      "output all expected values" in {

        val cmdCount = 1000

        val commandIds = 1.to(cmdCount).map(_.toString)

        val Handle(submissions, results, _, completionStreamMock) =
          runCommandTrackingFlow(allSubmissionsSuccessful)

        results.request(cmdCount.toLong - 1)

        commandIds.foreach { commandId =>
          submissions.sendNext(submission.copy(value = commandWithId(commandId)))
        }
        commandIds.foreach { commandId =>
          completionStreamMock.send(successfulCompletion(commandId))
        }

        results.expectNextUnorderedN(commandIds.map { commandId =>
          val successCompletion =
            Right(CompletionResponse.CompletionSuccess(commandId, "", successStatus))
          Ctx(context, successCompletion)
        })
        succeed
      }
    }

    "completion stream disconnects" should {
      "keep run and recover the completion subscription from a recent offset" in {
        val checkPointOffset = LedgerOffset(Absolute("checkpoint"))

        val Handle(submissions, results, _, completionStreamMock) =
          runCommandTrackingFlow(allSubmissionsSuccessful)

        def breakUntilOffsetArrives(): Future[Unit] =
          for {
            _ <- completionStreamMock.breakCompletionsStream()
            offset3 <- completionStreamMock.getLastOffset
            _ <-
              if (offset3 != checkPointOffset) breakUntilOffsetArrives()
              else Future.unit
          } yield ()

        def sendCommand(commandId: String) = {
          submissions.sendNext(submission.copy(value = commandWithId(commandId)))
          for {
            _ <- completionStreamMock.send(successfulCompletion(commandId))
            _ = results.request(1)
            _ = results.expectNext(
              Ctx(
                context,
                Right(CompletionResponse.CompletionSuccess(commandId, "", successStatus)),
              )
            )
          } yield ()
        }

        def checkOffset(expected: LedgerOffset) =
          for {
            offset <- completionStreamMock.getLastOffset
          } yield offset shouldEqual expected

        def sendCheckPoint(offset: LedgerOffset) =
          for {
            _ <- completionStreamMock.send(checkPoint(offset))
            _ = results.request(1)
          } yield ()

        for {
          _ <- checkOffset(LedgerOffset(Boundary(LEDGER_BEGIN)))
          _ <- sendCommand("1")
          _ <- sendCheckPoint(checkPointOffset)
          _ <- checkOffset(LedgerOffset(Boundary(LEDGER_BEGIN)))
          _ <- breakUntilOffsetArrives()
          _ <- checkOffset(checkPointOffset)
          _ <- sendCommand("2")
        } yield {
          succeed
        }
      }

    }

  }

  private def commandWithId(commandId: String) = {
    val request = submission.value
    request.copy(commands = request.commands.copy(commandId = commandId))
  }

  private def successfulCompletion(commandId: String) =
    CompletionStreamElement.CompletionElement(Completion(commandId, Some(successStatus)))

  private def checkPoint(ledgerOffset: LedgerOffset) =
    CompletionStreamElement.CheckpointElement(
      Checkpoint(
        Some(Timestamp(0, 0)),
        Some(ledgerOffset),
      )
    )

  private def runCommandTrackingFlow(
      submissionFlow: Flow[
        Ctx[(Int, String), CommandSubmission],
        Ctx[(Int, String), Try[Empty]],
        NotUsed,
      ],
      maximumExpiryTime: Duration = Duration.ofSeconds(10),
  ): Handle = {

    val completionsMock = new CompletionStreamMock()

    val trackingFlow =
      CommandTrackerFlow[Int, NotUsed](
        submissionFlow,
        completionsMock.createCompletionsSource,
        LedgerOffset(Boundary(LEDGER_BEGIN)),
        maximumExpiryTime,
      )

    val handle = submissionSource
      .viaMat(trackingFlow)(Keep.both)
      .toMat(resultSink) { (l, r) =>
        Handle(l._1, r, l._2.trackingMat, completionsMock)
      }
      .run()
    handle.completions.request(1L)
    handle
  }
}
