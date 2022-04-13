// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands.tracker

import java.time.{Duration, Instant}

import akka.stream.stage._
import akka.stream.{Attributes, Inlet, Outlet}
import com.daml.grpc.{GrpcException, GrpcStatus}
import com.daml.ledger.api.v1.command_completion_service.Checkpoint
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.client.services.commands.tracker.CommandTracker._
import com.daml.ledger.client.services.commands.tracker.CompletionResponse.{
  CompletionFailure,
  CompletionSuccess,
}
import com.daml.ledger.client.services.commands.{
  CommandSubmission,
  CompletionStreamElement,
  tracker,
}
import com.daml.util.Ctx
import com.google.protobuf.empty.Empty
import com.google.rpc.status.{Status => StatusProto}
import io.grpc.Status
import org.slf4j.LoggerFactory

import scala.collection.{immutable, mutable}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, Promise}
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success, Try}

/** Implements the logic of command tracking via two streams, a submit request and command completion stream.
  * These streams behave like standard `Flows`, applying tracking and processing logic along the way,
  * except that:
  * <ul><li>
  * if the command completion stream is failed, cancelled or completed, the submit request
  * stream is completed,
  * </li><li>
  * if the request stream is cancelled or completed, and there are no outstanding tracked commands,
  * the command stream is completed, and
  * </li><li>
  * if the request stream is failed, the stage completes and failure is transmitted to the result stream outlet.
  * </li></ul>
  * Materializes a future that completes when this stage completes or fails,
  * yielding a map containing any commands that were not completed.
  * </li></ul>
  * We also have an output for offsets, so the most recent offsets can be reused for recovery.
  */
private[commands] class CommandTracker[Context](
    maximumCommandTimeout: Duration,
    timeoutDetectionPeriod: FiniteDuration,
) extends GraphStageWithMaterializedValue[
      CommandTrackerShape[Context],
      Future[Map[TrackedCommandKey, Context]],
    ] {

  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  val submitRequestIn: Inlet[Ctx[Context, CommandSubmission]] =
    Inlet[Ctx[Context, CommandSubmission]]("submitRequestIn")
  val submitRequestOut: Outlet[Ctx[(Context, TrackedCommandKey), CommandSubmission]] =
    Outlet[Ctx[(Context, TrackedCommandKey), CommandSubmission]]("submitRequestOut")
  val commandResultIn
      : Inlet[Either[Ctx[(Context, TrackedCommandKey), Try[Empty]], CompletionStreamElement]] =
    Inlet[Either[Ctx[(Context, TrackedCommandKey), Try[Empty]], CompletionStreamElement]](
      "commandResultIn"
    )
  val resultOut: Outlet[ContextualizedCompletionResponse[Context]] =
    Outlet[ContextualizedCompletionResponse[Context]]("resultOut")
  val offsetOut: Outlet[LedgerOffset] =
    Outlet[LedgerOffset]("offsetOut")

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes
  ): (GraphStageLogic, Future[Map[TrackedCommandKey, Context]]) = {

    val promise = Promise[immutable.Map[TrackedCommandKey, Context]]()

    val logic: TimerGraphStageLogic = new TimerGraphStageLogic(shape) {

      val timeout_detection = "timeout-detection"

      override def preStart(): Unit = {
        scheduleWithFixedDelay(timeout_detection, timeoutDetectionPeriod, timeoutDetectionPeriod)
      }

      override protected def onTimer(timerKey: Any): Unit = {
        timerKey match {
          case `timeout_detection` =>
            val timeouts = getResponsesForTimeouts(Instant.now)
            if (timeouts.nonEmpty) emitMultiple(resultOut, timeouts.to(immutable.Iterable))
          case _ => // unknown timer, nothing to do
        }
      }

      private val pendingCommands = new mutable.HashMap[TrackedCommandKey, TrackingData[Context]]()

      setHandler(
        submitRequestOut,
        new OutHandler {
          override def onPull(): Unit = pull(submitRequestIn)

          override def onDownstreamFinish(cause: Throwable): Unit = {
            cancel(submitRequestIn)
            completeStageIfTerminal()
          }
        },
      )

      setHandler(
        submitRequestIn,
        new InHandler {
          override def onPush(): Unit = {
            val submitRequest = grab(submitRequestIn)
            registerSubmission(submitRequest)
            val commands = submitRequest.value.commands
            val submissionId = commands.submissionId
            val commandId = commands.commandId
            logger.trace(s"Submitted command $commandId in submission $submissionId.")
            push(
              submitRequestOut,
              submitRequest.enrich((context, _) =>
                context -> TrackedCommandKey(submissionId, commandId)
              ),
            )
          }

          override def onUpstreamFinish(): Unit = {
            logger.trace("Command upstream finished.")
            complete(submitRequestOut)
            completeStageIfTerminal()
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            fail(resultOut, ex)
          }
        },
      )

      setHandler(
        resultOut,
        new OutHandler {
          override def onPull(): Unit = if (!hasBeenPulled(commandResultIn)) pull(commandResultIn)
        },
      )

      setHandler(
        commandResultIn,
        new InHandler {

          /** This port was pulled by [[resultOut]], so that port expects an output.
            * If processing the input produces one, we push it through [[resultOut]], otherwise we pull this port again.
            * If multiple outputs are produced (possible with timeouts only) we get rid of them with emitMultiple.
            */
          override def onPush(): Unit = {
            grab(commandResultIn) match {
              case Left(submitResponse) =>
                pushResultOrPullCommandResultIn(handleSubmitResponse(submitResponse))

              case Right(CompletionStreamElement.CompletionElement(completion, checkpoint)) =>
                pushResultOrPullCommandResultIn(getResponseForCompletion(completion, checkpoint))

              case Right(CompletionStreamElement.CheckpointElement(checkpoint)) =>
                if (!hasBeenPulled(commandResultIn)) pull(commandResultIn)
                checkpoint.offset.foreach(emit(offsetOut, _))
            }

            completeStageIfTerminal()
          }
        },
      )

      setHandler(
        offsetOut,
        new OutHandler {
          override def onPull(): Unit =
            () // nothing to do here as the offset stream will be read with constant demand, storing the latest element
        },
      )

      private def pushResultOrPullCommandResultIn(
          completionResponse: Option[ContextualizedCompletionResponse[Context]]
      ): Unit = {
        // The command tracker detects timeouts outside the regular pull/push
        // mechanism of the input/output ports. Basically the timeout
        // detection jumps the line when emitting outputs on `resultOut`. If it
        // then processes a regular completion, it tries to push to `resultOut`
        // even though it hasn't been pulled again in the meantime. Using `emit`
        // instead of `push` when a completion arrives makes akka take care of
        // handling the signaling properly.
        completionResponse match {
          case Some(response) => emit(resultOut, response)
          case None =>
            if (!hasBeenPulled(commandResultIn)) {
              pull(commandResultIn)
            }
        }
      }

      private def completeStageIfTerminal(): Unit = {
        if (isClosed(submitRequestIn) && pendingCommands.isEmpty) {
          completeStage()
        }
      }

      import CommandTracker.nonTerminalCodes

      private def handleSubmitResponse(
          submitResponse: Ctx[(Context, TrackedCommandKey), Try[Empty]]
      ): Option[ContextualizedCompletionResponse[Context]] = {
        val Ctx((_, commandKey), value, _) = submitResponse
        value match {
          case Failure(GrpcException(status @ GrpcStatus(code, _), metadata))
              if !nonTerminalCodes(code) =>
            getResponseForTerminalStatusCode(
              commandKey,
              GrpcStatus.toProto(status, metadata),
            )
          case Failure(throwable) =>
            logger.warn(
              s"Service responded with error for submitting command with context ${submitResponse.context}. Status of command is unknown. watching for completion...",
              throwable,
            )
            None
          case Success(_) =>
            logger.trace(
              s"Received confirmation that command ${commandKey.commandId} from submission ${commandKey.submissionId} was accepted."
            )
            None
        }
      }

      private def registerSubmission(submission: Ctx[Context, CommandSubmission]): Unit = {
        val commands = submission.value.commands
        val submissionId = commands.submissionId
        val commandId = commands.commandId
        logger.trace(s"Begin tracking of command $commandId for submission $submissionId.")
        if (submissionId.isEmpty) {
          throw new IllegalArgumentException(
            s"The submission ID for the command ID $commandId is empty. This should not happen."
          )
        }
        if (pendingCommands.contains(TrackedCommandKey(submissionId, commandId))) {
          // TODO return an error identical to the server side duplicate command error once that's defined.
          throw new IllegalStateException(
            s"A command $commandId from a submission $submissionId is already being tracked. CommandIds submitted to the CommandTracker must be unique."
          ) with NoStackTrace
        }
        val commandTimeout = submission.value.timeout match {
          case Some(timeout) => durationOrdering.min(timeout, maximumCommandTimeout)
          case None => maximumCommandTimeout
        }
        val trackingData = TrackingData(
          commandId = commandId,
          commandTimeout = Instant.now().plus(commandTimeout),
          context = submission.context,
        )
        pendingCommands += TrackedCommandKey(submissionId, commandId) -> trackingData
        ()
      }

      private def getResponsesForTimeouts(
          instant: Instant
      ): Seq[ContextualizedCompletionResponse[Context]] = {
        logger.trace("Checking timeouts at {}", instant)
        pendingCommands.view.flatMap { case (commandKey, trackingData) =>
          if (trackingData.commandTimeout.isBefore(instant)) {
            pendingCommands -= commandKey
            logger.info(
              s"Command {} from submission {} (command timeout {}) timed out at checkpoint {}.",
              commandKey.commandId,
              commandKey.submissionId,
              trackingData.commandTimeout,
              instant,
            )
            List(
              Ctx(
                trackingData.context,
                Left(CompletionResponse.TimeoutResponse(commandId = trackingData.commandId)),
              )
            )
          } else {
            Nil
          }
        }.toSeq
      }

      private def getResponseForCompletion(
          completion: Completion,
          checkpoint: Option[Checkpoint],
      ): Option[ContextualizedCompletionResponse[Context]] = {
        val commandId = completion.commandId
        val maybeSubmissionId = Option(completion.submissionId).filter(_.nonEmpty)
        logger.trace {
          val completionDescription = completion.status match {
            case Some(StatusProto(code, _, _, _)) if code == Status.Code.OK.value =>
              "successful completion of command"
            case _ => "failed completion of command"
          }
          s"Handling $completionDescription $commandId from submission $maybeSubmissionId."
        }

        maybeSubmissionId
          .map { submissionId =>
            val key = TrackedCommandKey(submissionId, completion.commandId)
            val trackedCommandForCompletion = pendingCommands.remove(key)
            trackedCommandForCompletion.map(trackingData =>
              Ctx(
                trackingData.context,
                tracker.CompletionResponse(completion = completion, checkpoint = checkpoint),
              )
            )
          }
          .getOrElse {
            logger.warn("Ignoring a completion with an empty submission ID.")
            None
          }
      }

      private def getResponseForTerminalStatusCode(
          commandKey: TrackedCommandKey,
          status: StatusProto,
      ): Option[ContextualizedCompletionResponse[Context]] = {
        logger.trace(
          s"Handling failure of command ${commandKey.commandId} from submission ${commandKey.submissionId}."
        )
        pendingCommands
          .remove(commandKey)
          .map { t =>
            Ctx(
              t.context,
              tracker.CompletionResponse(
                completion = Completion(
                  commandKey.commandId,
                  Some(status),
                  submissionId = commandKey.submissionId,
                ),
                checkpoint = None,
              ),
            )
          }
          .orElse {
            logger.trace(
              s"Platform signaled failure for unknown command ${commandKey.commandId} from submission ${commandKey.submissionId}."
            )
            None
          }
      }

      override def postStop(): Unit = {
        promise.tryComplete(Success(pendingCommands.view.map { case (k, v) =>
          k -> v.context
        }.toMap))
        super.postStop()
      }
    }

    logic -> promise.future
  }

  override def shape: CommandTrackerShape[Context] =
    CommandTrackerShape(submitRequestIn, submitRequestOut, commandResultIn, resultOut, offsetOut)

}

object CommandTracker {
  type ContextualizedCompletionResponse[Context] =
    Ctx[Context, Either[CompletionFailure, CompletionSuccess]]

  private val durationOrdering = implicitly[Ordering[Duration]]

  private val nonTerminalCodes =
    Set(Status.Code.UNKNOWN, Status.Code.INTERNAL, Status.Code.OK)
}
