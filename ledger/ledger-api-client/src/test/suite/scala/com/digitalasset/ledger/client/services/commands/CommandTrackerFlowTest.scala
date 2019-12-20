// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.services.commands

import java.time.{Instant, Duration => JDuration}
import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Flow, Keep, Source, SourceQueueWithComplete}
import akka.stream.testkit.javadsl.TestSink
import akka.stream.testkit.scaladsl.TestSource
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import com.digitalasset.api.util.TimestampConversion._
import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.digitalasset.ledger.api.v1.command_completion_service.Checkpoint
import com.digitalasset.ledger.api.v1.command_submission_service._
import com.digitalasset.ledger.api.v1.commands.Commands
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset.LedgerBoundary.LEDGER_BEGIN
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset.Value.{Absolute, Boundary}
import com.digitalasset.util.Ctx
import com.google.protobuf.empty.Empty
import com.google.protobuf.timestamp.Timestamp
import com.google.rpc.code._
import com.google.rpc.status.Status
import io.grpc.StatusRuntimeException
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{AsyncWordSpec, Matchers, OptionValues}

import scala.concurrent.duration._
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
    : Flow[Ctx[(Int, String), SubmitRequest], Ctx[(Int, String), Try[Empty]], NotUsed] =
    Flow[C[SubmitRequest]].map {
      _.map(_ => Success(Empty.defaultInstance))
    }

  private val shortDuration = JDuration.ofSeconds(1L)

  private lazy val submissionSource = TestSource.probe[Ctx[Int, SubmitRequest]]
  private lazy val resultSink = TestSink.probe[Ctx[Int, Completion]](system)

  private val mrt = Instant.EPOCH.plus(shortDuration)
  private val commandId = "commandId"
  private val context = 1
  private val submitRequest = Ctx(
    context,
    SubmitRequest(Some(Commands(commandId = commandId, maximumRecordTime = Some(fromInstant(mrt)))))
  )

  private case class Handle(
      submissions: TestPublisher.Probe[Ctx[Int, SubmitRequest]],
      completions: TestSubscriber.Probe[Ctx[Int, Completion]],
      whatever: Future[Map[String, Int]],
      completionsStreamMock: CompletionStreamMock)

  private class CompletionStreamMock() {

    case class State(
        queue: SourceQueueWithComplete[CompletionStreamElement],
        startOffset: LedgerOffset)

    private implicit val ec = DirectExecutionContext
    private val stateRef = new AtomicReference[Promise[State]](Promise[State]())

    def createCompletionsSource(
        ledgerOffset: LedgerOffset): Source[CompletionStreamElement, NotUsed] = {
      val (queue, completionSource) =
        Source
          .queue[CompletionStreamElement](Int.MaxValue, OverflowStrategy.backpressure)
          .preMaterialize()
      stateRef.get().success(State(queue, ledgerOffset))
      completionSource
    }

    def send(elem: CompletionStreamElement) =
      for {
        state <- stateRef.get().future
        res <- state.queue.offer(elem)
      } yield (res)

    def breakCompletionsStream(): Future[Unit] =
      stateRef
        .getAndSet(Promise[State]())
        .future
        .map(state => state.queue.fail(new RuntimeException("boom")))

    def getLastOffset = stateRef.get().future.map(_.startOffset)

  }

  "Command tracking flow" when {

    "two commands are submitted with the same ID" should {

      "fail the stream" in {

        val Handle(submissions, results, _, _) =
          runCommandTrackingFlow(allSubmissionsSuccessful)

        submissions.sendNext(submitRequest)
        submissions.sendNext(submitRequest)

        results.expectError() shouldBe a[RuntimeException]
      }
    }

    "the stream fails" should {

      "expose internal state as materialized value" in {

        val Handle(submissions, _, unhandledF, _) =
          runCommandTrackingFlow(allSubmissionsSuccessful)

        submissions.sendNext(submitRequest)
        submissions.sendNext(submitRequest)

        whenReady(unhandledF) { unhandled =>
          unhandled should have size 1
          unhandled should contain(
            submitRequest.value.commands.value.commandId -> submitRequest.context)
        }
      }
    }

    "the stream completes" should {

      "expose internal state as materialized value" in {

        val Handle(submissions, results, unhandledF, _) =
          runCommandTrackingFlow(allSubmissionsSuccessful)
        val otherCommandId = "otherId"

        submissions.sendNext(submitRequest.map(request =>
          request.copy(commands = request.commands.map(_.copy(commandId = otherCommandId)))))

        results.cancel()
        whenReady(unhandledF) { unhandled =>
          unhandled should have size 1
          unhandled should contain(otherCommandId -> submitRequest.context)
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

        submissions.sendNext(submitRequest)
        submissions.sendComplete()

        results.expectNoMessage(1.second)
        succeed
      }
    }

    "grpc error arrives for submission" should {

      "output it as a completion if terminal" in {

        val Handle(submissions, results, _, _) =
          runCommandTrackingFlow(Flow[C[SubmitRequest]].map {
            _.map(_ => Failure(new StatusRuntimeException(io.grpc.Status.RESOURCE_EXHAUSTED)))
          })

        submissions.sendNext(submitRequest)

        val failureCompletion =
          Completion(
            commandId,
            Some(Status(Code.RESOURCE_EXHAUSTED.value)),
            traceContext = submitRequest.value.traceContext)

        results.expectNext(Ctx(context, failureCompletion))
        succeed
      }

      "swallow error ifnot terminal" in {

        val Handle(submissions, results, _, completionStreamMock) =
          runCommandTrackingFlow(Flow[C[SubmitRequest]].map {
            _.map(_ => Failure(new StatusRuntimeException(io.grpc.Status.UNKNOWN)))
          })

        submissions.sendNext(submitRequest)

        results.expectNoMessage(3.seconds)

        val completion =
          Completion(
            commandId,
            Some(Status(Code.ABORTED.value)),
            traceContext = submitRequest.value.traceContext)
        completionStreamMock.send(CompletionStreamElement.CompletionElement(completion))
        results.requestNext().value shouldEqual completion
        succeed
      }

      "swallow error if not terminal, then output completion when it arrives" in {

        val Handle(submissions, results, _, completionStreamMock) =
          runCommandTrackingFlow(Flow[C[SubmitRequest]].map {
            _.map(_ => Failure(new StatusRuntimeException(io.grpc.Status.UNKNOWN)))
          })

        submissions.sendNext(submitRequest)

        val completion =
          Completion(
            commandId,
            Some(Status(Code.ABORTED.value)),
            traceContext = submitRequest.value.traceContext)
        completionStreamMock.send(CompletionStreamElement.CompletionElement(completion))
        results.requestNext().value shouldEqual completion
      }

    }

    "no completion arrives" should {

      "not timeout the command while MRT <= RT" in {

        val Handle(submissions, results, _, completionStreamMock) =
          runCommandTrackingFlow(allSubmissionsSuccessful)

        submissions.sendNext(submitRequest)

        completionStreamMock.send(
          CompletionStreamElement.CheckpointElement(Checkpoint(Some(fromInstant(mrt)))))

        results.expectNoMessage(1.second)
        succeed
      }

      "timeout the command when the MRT passes" in {

        val Handle(submission, results, _, completionStreamMock) =
          runCommandTrackingFlow(allSubmissionsSuccessful)

        submission.sendNext(submitRequest)

        completionStreamMock.send(
          CompletionStreamElement.CheckpointElement(
            Checkpoint(Some(fromInstant(mrt.plus(shortDuration))))))

        results.expectNext(
          Ctx(context, Completion(commandId, Some(Status(Code.ABORTED.value, "Timeout")))))
        succeed
      }
    }

    "successful completion arrives" should {

      "output the completion" in {

        val Handle(submissions, results, _, completionStreamMock) =
          runCommandTrackingFlow(allSubmissionsSuccessful)

        submissions.sendNext(submitRequest)

        completionStreamMock.send(successfulCompletion(commandId))

        results.expectNext(Ctx(context, Completion(commandId, Some(Status()))))
        succeed
      }
    }

    "duplicate completion arrives for a particular command" should {

      "output the completion only once" in {

        val Handle(submissions, results, _, completionStreamMock) =
          runCommandTrackingFlow(allSubmissionsSuccessful)

        submissions.sendNext(submitRequest)

        completionStreamMock.send(successfulCompletion(commandId))
        completionStreamMock.send(successfulCompletion(commandId))

        results.expectNext(Ctx(context, Completion(commandId, Some(Status()))))
        results.expectNoMessage(1.second)
        succeed
      }
    }

    "failed completion arrives" should {

      "output the completion" in {

        val Handle(submissions, results, _, completionStreamMock) =
          runCommandTrackingFlow(allSubmissionsSuccessful)

        submissions.sendNext(submitRequest)

        val failureCompletion =
          Completion(
            commandId,
            Some(Status(Code.INVALID_ARGUMENT.value)),
            traceContext = submitRequest.value.traceContext)
        completionStreamMock.send(CompletionStreamElement.CompletionElement(failureCompletion))

        results.expectNext(Ctx(context, failureCompletion))
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
          submissions.sendNext(submitRequest.copy(value = commandWithId(commandId)))
        }
        commandIds.foreach { commandId =>
          completionStreamMock.send(successfulCompletion(commandId))
        }

        results.expectNextUnorderedN(commandIds.map { commandId =>
          val successCompletion =
            Completion(commandId, Some(Status()), traceContext = submitRequest.value.traceContext)
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
            _ <- if (offset3 != checkPointOffset) breakUntilOffsetArrives()
            else Future.successful(())
          } yield ()

        def sendCommand(commandId: String) = {
          submissions.sendNext(submitRequest.copy(value = commandWithId(commandId)))
          for {
            _ <- completionStreamMock.send(successfulCompletion(commandId))
            _ = results.request(1)
            _ = results.expectNext(
              Ctx(
                context,
                Completion(
                  commandId,
                  Some(Status()),
                  traceContext = submitRequest.value.traceContext)))
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
    val request = submitRequest.value
    request.copy(commands = request.commands.map(_.copy(commandId = commandId)))
  }

  private def successfulCompletion(commandId: String) =
    CompletionStreamElement.CompletionElement(Completion(commandId, Some(Status())))

  private def checkPoint(ledgerOffset: LedgerOffset) =
    CompletionStreamElement.CheckpointElement(
      Checkpoint(
        Some(Timestamp(0, 0)),
        Some(ledgerOffset)
      ))

  private def runCommandTrackingFlow(
      submissionFlow: Flow[
        Ctx[(Int, String), SubmitRequest],
        Ctx[(Int, String), Try[Empty]],
        NotUsed]) = {

    val completionsMock = new CompletionStreamMock()

    val trackingFlow =
      CommandTrackerFlow[Int, NotUsed](
        submissionFlow,
        completionsMock.createCompletionsSource,
        LedgerOffset(Boundary(LEDGER_BEGIN)))

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
