// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands

import java.time.{Duration, Instant}
import java.util.concurrent.atomic.AtomicReference

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Source, SourceQueueWithComplete}
import org.apache.pekko.stream.testkit.javadsl.TestSink
import org.apache.pekko.stream.testkit.scaladsl.TestSource
import org.apache.pekko.stream.testkit.{TestPublisher, TestSubscriber}
import org.apache.pekko.stream.{OverflowStrategy, QueueOfferResult}
import com.daml.api.util.TimestampConversion._
import com.daml.concurrent.ExecutionContext
import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.ledger.api.v1.command_completion_service.Checkpoint
import com.daml.ledger.api.v1.commands.Commands
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset.LedgerBoundary.LEDGER_BEGIN
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset.Value.{Absolute, Boundary}
import com.daml.ledger.client.services.commands.tracker.CompletionResponse.{
  CompletionFailure,
  CompletionSuccess,
  NotOkResponse,
}
import com.daml.ledger.client.services.commands.tracker.{CompletionResponse, TrackedCommandKey}
import com.daml.util.Ctx
import com.google.protobuf.empty.Empty
import com.google.protobuf.timestamp.Timestamp
import com.google.rpc.code._
import com.google.rpc.status.{Status => StatusProto}
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
    with PekkoBeforeAndAfterAll
    with ScalaFutures {

  type C[Value] = Ctx[(Int, TrackedCommandKey), Value]

  private val allSubmissionsSuccessful: Flow[Ctx[(Int, TrackedCommandKey), CommandSubmission], Ctx[
    (Int, TrackedCommandKey),
    Try[Empty],
  ], NotUsed] =
    Flow[C[CommandSubmission]].map {
      _.map(_ => Success(Empty.defaultInstance))
    }

  private val shortDuration = Duration.ofSeconds(1L)

  private lazy val submissionSource = TestSource.probe[Ctx[Int, CommandSubmission]]
  private lazy val resultSink =
    TestSink.probe[Ctx[Int, Either[CompletionFailure, CompletionSuccess]]](system)

  private val mrt = Instant.EPOCH.plus(shortDuration)
  private val submissionId = "submissionId"
  private val commandId = "commandId"
  private val abortedCompletion =
    Completion(
      commandId,
      Some(StatusProto(Code.ABORTED.value)),
      submissionId = submissionId,
    )
  private val successStatus = StatusProto(Code.OK.value)
  private val context = 1
  private val submission = newSubmission(submissionId, commandId)

  private def newSubmission(
      submissionId: String,
      commandId: String,
      timeout: Option[Duration] = None,
  ) =
    Ctx(
      context,
      CommandSubmission(Commands(commandId = commandId, submissionId = submissionId), timeout),
    )

  private case class Handle(
      submissions: TestPublisher.Probe[Ctx[Int, CommandSubmission]],
      completions: TestSubscriber.Probe[Ctx[Int, Either[CompletionFailure, CompletionSuccess]]],
      whatever: Future[Map[TrackedCommandKey, Int]],
      completionsStreamMock: CompletionStreamMock,
  )

  private class CompletionStreamMock() {

    case class State(
        queue: SourceQueueWithComplete[CompletionStreamElement],
        startOffset: LedgerOffset,
    )

    private implicit val ec: ExecutionContext[Nothing] = ExecutionContext.parasitic
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

    "a command is submitted without a submission id" should {

      "throw an exception" in {
        val Handle(submissions, results, _, _) =
          runCommandTrackingFlow(allSubmissionsSuccessful)

        submissions.sendNext(newSubmission("", commandId))

        val actualException = results.expectError()
        actualException shouldBe an[IllegalArgumentException]
        actualException.getMessage shouldBe s"The submission ID for the command ID $commandId is empty. This should not happen."
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
            TrackedCommandKey(submissionId, commandId) -> submission.context
          )
        }
      }
    }

    "the stream completes" should {

      "expose internal state as materialized value" in {

        val Handle(submissions, results, unhandledF, _) =
          runCommandTrackingFlow(allSubmissionsSuccessful)
        val otherSubmissionId = "otherSubmissionId"
        val otherCommandId = "otherCommandId"

        submissions.sendNext(newSubmission(otherSubmissionId, otherCommandId))

        results.cancel()
        whenReady(unhandledF) { unhandled =>
          unhandled should have size 1
          unhandled should contain(
            TrackedCommandKey(otherSubmissionId, otherCommandId) -> submission.context
          )
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

        results.expectNext(Ctx(context, Left(failureCompletion(Code.RESOURCE_EXHAUSTED))))
        succeed
      }

      "swallow error if not terminal" in {

        val Handle(submissions, results, _, completionStreamMock) =
          runCommandTrackingFlow(Flow[C[CommandSubmission]].map {
            _.map(_ => Failure(new StatusRuntimeException(io.grpc.Status.UNKNOWN)))
          })

        submissions.sendNext(submission)

        results.expectNoMessage(3.seconds)

        completionStreamMock.send(
          CompletionStreamElement.CompletionElement(abortedCompletion, None)
        )
        results.requestNext().value shouldEqual Left(
          failureCompletion(Code.ABORTED)
        )
      }

      "swallow error if not terminal, then output completion when it arrives" in {

        val Handle(submissions, results, _, completionStreamMock) =
          runCommandTrackingFlow(Flow[C[CommandSubmission]].map {
            _.map(_ => Failure(new StatusRuntimeException(io.grpc.Status.UNKNOWN)))
          })

        submissions.sendNext(submission)

        completionStreamMock.send(
          CompletionStreamElement.CompletionElement(abortedCompletion, None)
        )
        results.requestNext().value shouldEqual Left(
          failureCompletion(Code.ABORTED)
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

      "timeout the command when the timeout passes" in {
        val Handle(submissions, results, _, _) = runCommandTrackingFlow(allSubmissionsSuccessful)

        submissions.sendNext(
          newSubmission(submissionId, commandId, timeout = Some(Duration.ofMillis(100)))
        )

        results.expectNext(
          500.milliseconds,
          Ctx(context, Left(CompletionResponse.TimeoutResponse(commandId))),
        )
        succeed
      }

      "use the maximum command timeout, if provided" in {
        val Handle(submissions, results, _, _) = runCommandTrackingFlow(
          allSubmissionsSuccessful,
          maximumCommandTimeout = Duration.ofMillis(500),
        )

        submissions.sendNext(submission)

        results.expectNext(
          1.second,
          Ctx(context, Left(CompletionResponse.TimeoutResponse(commandId))),
        )
        succeed
      }

      "cap the timeout at the maximum command timeout" in {
        val Handle(submissions, results, _, _) = runCommandTrackingFlow(
          allSubmissionsSuccessful,
          maximumCommandTimeout = Duration.ofMillis(100),
        )

        submissions.sendNext(
          newSubmission(submissionId, commandId, timeout = Some(Duration.ofSeconds(10)))
        )

        results.expectNext(
          500.millis,
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

        completionStreamMock.send(successfulStreamCompletion(submissionId, commandId))

        results.expectNext(
          Ctx(
            context,
            Right(successCompletion()),
          )
        )
        succeed
      }

      "after the timeout" in {
        val Handle(submissions, results, _, completionStreamMock) =
          runCommandTrackingFlow(allSubmissionsSuccessful)
        val timedOutSubmissionId = "timedOutSubmissionId"
        val timedOutCommandId = "timedOutCommandId"

        submissions.sendNext(
          newSubmission(timedOutSubmissionId, timedOutCommandId, timeout = Some(shortDuration))
        )

        results.expectNext(
          shortDuration.getSeconds.seconds * 3,
          Ctx(context, Left(CompletionResponse.TimeoutResponse(timedOutCommandId))),
        )

        // since the command timed out before, the tracker shouldn't send the completion through
        completionStreamMock.send(
          successfulStreamCompletion(timedOutSubmissionId, timedOutCommandId)
        )
        results.request(1)
        results.expectNoMessage()
        succeed
      }

      "after another command has timed out" in {
        val Handle(submissions, results, _, completionStreamMock) =
          runCommandTrackingFlow(allSubmissionsSuccessful)
        val timedOutSubmissionId = "timedOutSubmissionId"
        val timedOutCommandId = "timedOutCommandId"
        val submitRequestShortDedupTime =
          newSubmission(timedOutSubmissionId, timedOutCommandId, timeout = Some(shortDuration))

        // we send 2 requests
        submissions.sendNext(submitRequestShortDedupTime)
        submissions.sendNext(submission)

        // the tracker observes the timeout before the completion, thus "consuming" the pull on the result output
        results.expectNext(
          3.seconds,
          Ctx(context, Left(CompletionResponse.TimeoutResponse(timedOutCommandId))),
        )
        // we now receive a completion
        completionStreamMock.send(successfulStreamCompletion(submissionId, commandId))
        // because the out-of-band timeout completion consumed the previous pull on `results`,
        // we don't expect a message until we request one.
        // The order below is important to reproduce the issue described in DPP-285.
        results.expectNoMessage()
        results.request(1)
        results.expectNext(
          Ctx(context, Right(successCompletion()))
        )
        succeed
      }

    }

    "duplicate completion arrives for a particular command" should {

      "output the completion only once" in {

        val Handle(submissions, results, _, completionStreamMock) =
          runCommandTrackingFlow(allSubmissionsSuccessful)

        submissions.sendNext(submission)

        completionStreamMock.send(successfulStreamCompletion(submissionId, commandId))
        completionStreamMock.send(successfulStreamCompletion(submissionId, commandId))

        results.expectNext(
          Ctx(context, Right(successCompletion()))
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

        val status = StatusProto(Code.INVALID_ARGUMENT.value)
        val failedCompletion =
          Completion(
            commandId,
            Some(status),
            submissionId = submissionId,
          )
        completionStreamMock.send(CompletionStreamElement.CompletionElement(failedCompletion, None))

        results.expectNext(
          Ctx(
            context,
            Left(
              failureCompletion(Code.INVALID_ARGUMENT)
            ),
          )
        )
        succeed
      }
    }

    "a multitude of successful completions arrive for submitted commands" should {

      "output all expected values" in {
        val cmdCount = 1000

        val identifiers =
          1.to(cmdCount).map(_.toString).map(id => s"submission-$id" -> s"command-$id")

        val Handle(submissions, results, _, completionStreamMock) =
          runCommandTrackingFlow(allSubmissionsSuccessful)

        results.request(cmdCount.toLong - 1)

        identifiers.foreach { case (submissionId, commandId) =>
          submissions.sendNext(submission.copy(value = commandWithIds(submissionId, commandId)))
        }
        identifiers.foreach { case (submissionId, commandId) =>
          completionStreamMock.send(successfulStreamCompletion(submissionId, commandId))
        }

        results.expectNextUnorderedN(identifiers.map { case (submissionId, commandId) =>
          val completionSuccess = successCompletion()
          Ctx(
            context,
            Right(
              completionSuccess.copy(completion =
                completionSuccess.completion
                  .update(_.commandId := commandId, _.submissionId := submissionId)
              )
            ),
          )
        })
        succeed
      }
    }

    "successful completions arrive for the same command submitted twice with different submission IDs" should {

      "output two successful responses" in {
        val Handle(submissions, results, _, completionStreamMock) =
          runCommandTrackingFlow(allSubmissionsSuccessful)

        results.request(2)

        val submissionId1 = "submissionId"
        val submissionId2 = "anotherSubmissionId"

        submissions.sendNext(newSubmission(submissionId1, commandId))
        submissions.sendNext(newSubmission(submissionId2, commandId))

        completionStreamMock.send(successfulStreamCompletion(submissionId1, commandId))
        completionStreamMock.send(successfulStreamCompletion(submissionId2, commandId))

        results.expectNextUnordered(
          Ctx(context, Right(successCompletion(submissionId = submissionId1))),
          Ctx(context, Right(successCompletion(submissionId = submissionId2))),
        )
        succeed
      }
    }

    "completions with empty and nonempty submission IDs arrive" should {

      "ignore a completion with an empty submission ID and output a successful response" in {
        val Handle(submissions, results, _, completionStreamMock) =
          runCommandTrackingFlow(allSubmissionsSuccessful)

        results.request(2)

        submissions.sendNext(submission)

        completionStreamMock.send(successfulStreamCompletion("", commandId))
        completionStreamMock.send(successfulStreamCompletion(submissionId, commandId))

        results.expectNext(
          Ctx(context, Right(successCompletion(submissionId = submissionId)))
        )
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

        def sendCommand(submissionId: String, commandId: String) = {
          submissions.sendNext(submission.copy(value = commandWithIds(submissionId, commandId)))
          for {
            _ <- completionStreamMock.send(successfulStreamCompletion(submissionId, commandId))
            _ = results.request(1)
            _ = results.expectNext(
              Ctx(
                context,
                Right(
                  successCompletion(commandId, submissionId)
                ),
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
          _ <- sendCommand("submission-1", "command-1")
          _ <- sendCheckPoint(checkPointOffset)
          _ <- checkOffset(LedgerOffset(Boundary(LEDGER_BEGIN)))
          _ <- breakUntilOffsetArrives()
          _ <- checkOffset(checkPointOffset)
          _ <- sendCommand("submission-2", "command-2")
        } yield {
          succeed
        }
      }

    }

  }

  private def successCompletion(
      commandId: String = commandId,
      submissionId: String = submissionId,
  ) =
    CompletionResponse.CompletionSuccess(
      Completion(commandId, Some(successStatus), submissionId = submissionId),
      None,
    )

  private def failureCompletion(
      code: Code,
      message: String = "",
      submissionId: String = submissionId,
  ): CompletionFailure =
    NotOkResponse(
      Completion(
        commandId = commandId,
        status = Some(StatusProto(code.value, message)),
        submissionId = submissionId,
      ),
      None,
    )

  private def commandWithIds(submissionId: String, commandId: String) = {
    val request = submission.value
    request.copy(commands =
      request.commands.copy(commandId = commandId, submissionId = submissionId)
    )
  }

  private def successfulStreamCompletion(submissionId: String, commandId: String) =
    CompletionStreamElement.CompletionElement(
      Completion(commandId, Some(successStatus), submissionId = submissionId),
      None,
    )

  private def checkPoint(ledgerOffset: LedgerOffset) =
    CompletionStreamElement.CheckpointElement(
      Checkpoint(
        Some(Timestamp(0, 0)),
        Some(ledgerOffset),
      )
    )

  private def runCommandTrackingFlow(
      submissionFlow: Flow[
        Ctx[(Int, TrackedCommandKey), CommandSubmission],
        Ctx[(Int, TrackedCommandKey), Try[Empty]],
        NotUsed,
      ],
      maximumCommandTimeout: Duration = Duration.ofSeconds(10),
  ): Handle = {

    val completionsMock = new CompletionStreamMock()

    val trackingFlow =
      CommandTrackerFlow[Int, NotUsed](
        commandSubmissionFlow = submissionFlow,
        createCommandCompletionSource = completionsMock.createCompletionsSource,
        startingOffset = LedgerOffset(Boundary(LEDGER_BEGIN)),
        maximumCommandTimeout = maximumCommandTimeout,
        timeoutDetectionPeriod = 1.millisecond,
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
