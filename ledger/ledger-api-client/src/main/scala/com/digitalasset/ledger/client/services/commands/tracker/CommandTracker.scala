// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands.tracker

import java.time.{Instant, Duration => JDuration}

import akka.stream.stage._
import akka.stream.{Attributes, Inlet, Outlet}
import com.daml.grpc.{GrpcException, GrpcStatus}
import com.daml.ledger.api.v1.command_submission_service._
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.client.services.commands.tracker.CompletionResponse.{
  CompletionFailure,
  CompletionSuccess,
  NotOkResponse,
}
import com.daml.ledger.client.services.commands.{CompletionStreamElement, tracker}
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.util.Ctx
import com.google.protobuf.empty.Empty
import com.google.rpc.status.{Status => StatusProto}
import io.grpc.Status
import org.slf4j.LoggerFactory

import scala.collection.compat._
import scala.collection.{immutable, mutable}
import scala.concurrent.duration.DurationInt
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
// TODO(mthvedt): This should have unit tests.
private[commands] class CommandTracker[Context](maxDeduplicationTime: () => Option[JDuration])
    extends GraphStageWithMaterializedValue[CommandTrackerShape[Context], Future[
      immutable.Map[String, Context]
    ]] {

  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  val submitRequestIn: Inlet[Ctx[Context, SubmitRequest]] =
    Inlet[Ctx[Context, SubmitRequest]]("submitRequestIn")
  val submitRequestOut: Outlet[Ctx[(Context, String), SubmitRequest]] =
    Outlet[Ctx[(Context, String), SubmitRequest]]("submitRequestOut")
  val commandResultIn: Inlet[Either[Ctx[(Context, String), Try[Empty]], CompletionStreamElement]] =
    Inlet[Either[Ctx[(Context, String), Try[Empty]], CompletionStreamElement]]("commandResultIn")
  val resultOut: Outlet[Ctx[Context, Either[CompletionFailure, CompletionSuccess]]] =
    Outlet[Ctx[Context, Either[CompletionFailure, CompletionSuccess]]]("resultOut")
  val offsetOut: Outlet[LedgerOffset] =
    Outlet[LedgerOffset]("offsetOut")

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes
  ): (GraphStageLogic, Future[Map[String, Context]]) = {

    val promise = Promise[immutable.Map[String, Context]]()

    val logic: TimerGraphStageLogic = new TimerGraphStageLogic(shape) {

      val timeout_detection = "timeout-detection"
      override def preStart(): Unit = {
        scheduleWithFixedDelay(timeout_detection, 1.second, 1.second)

      }

      override protected def onTimer(timerKey: Any): Unit = {
        timerKey match {
          case `timeout_detection` =>
            val timeouts = getOutputForTimeout(Instant.now)
            if (timeouts.nonEmpty) emitMultiple(resultOut, timeouts)
          case _ => // unknown timer, nothing to do
        }
      }

      private val pendingCommands = new mutable.HashMap[String, TrackingData[Context]]()

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
            logger.trace(
              "Submitted command {}",
              submitRequest.value.getCommands.commandId,
            )
            push(submitRequestOut, submitRequest.enrich(_ -> _.getCommands.commandId))
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

              case Right(CompletionStreamElement.CompletionElement(completion)) =>
                pushResultOrPullCommandResultIn(getOutputForCompletion(completion))

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
            () //nothing to do here as the offset stream will be read with constant demand, storing the latest element
        },
      )

      private def pushResultOrPullCommandResultIn(
          compl: Option[Ctx[Context, Either[CompletionFailure, CompletionSuccess]]]
      ): Unit = {
        // The command tracker detects timeouts outside the regular pull/push
        // mechanism of the input/output ports. Basically the timeout
        // detection jumps the line when emitting outputs on `resultOut`. If it
        // then processes a regular completion, it tries to push to `resultOut`
        // even though it hasn't been pulled again in the meantime. Using `emit`
        // instead of `push` when a completion arrives makes akka take care of
        // handling the signaling properly.
        compl.fold(if (!hasBeenPulled(commandResultIn)) pull(commandResultIn))(emit(resultOut, _))
      }

      private def completeStageIfTerminal(): Unit = {
        if (isClosed(submitRequestIn) && pendingCommands.isEmpty) {
          completeStage()
        }
      }

      import CommandTracker.nonTerminalCodes

      private def handleSubmitResponse(submitResponse: Ctx[(Context, String), Try[Empty]]) = {
        val Ctx((_, commandId), value, _) = submitResponse
        value match {
          case Failure(GrpcException(status @ GrpcStatus(code, _), _)) if !nonTerminalCodes(code) =>
            getOutputForTerminalStatusCode(commandId, GrpcStatus.toProto(status))
          case Failure(throwable) =>
            logger.warn(
              s"Service responded with error for submitting command with context ${submitResponse.context}. Status of command is unknown. watching for completion...",
              throwable,
            )
            None
          case Success(_) =>
            logger.trace("Received confirmation that command {} was accepted.", commandId)
            None
        }
      }

      private def registerSubmission(submitRequest: Ctx[Context, SubmitRequest]): Unit =
        submitRequest.value.commands match {
          case None =>
            throw new IllegalArgumentException(
              "Commands field is missing from received SubmitRequest in CommandTracker"
            ) with NoStackTrace
          case Some(commands) =>
            val commandId = commands.commandId
            logger.trace("Begin tracking of command {}", commandId)
            if (pendingCommands.contains(commandId)) {
              // TODO return an error identical to the server side duplicate command error once that's defined.
              throw new IllegalStateException(
                s"A command with id $commandId is already being tracked. CommandIds submitted to the CommandTracker must be unique."
              ) with NoStackTrace
            }
            maxDeduplicationTime() match {
              case None =>
                emit(
                  resultOut,
                  submitRequest.map { _ =>
                    val status = GrpcStatus.toProto(ErrorFactories.missingLedgerConfig().getStatus)
                    Left(NotOkResponse(commandId, status))
                  },
                )
              case Some(maxDedup) =>
                val deduplicationDuration = commands.deduplicationTime
                  .map(d => JDuration.ofSeconds(d.seconds, d.nanos.toLong))
                  .getOrElse(maxDedup)
                val trackingData = TrackingData(
                  commandId = commandId,
                  commandTimeout = Instant.now().plus(deduplicationDuration),
                  context = submitRequest.context,
                )
                pendingCommands += commandId -> trackingData
            }
            ()
        }

      private def getOutputForTimeout(instant: Instant) = {
        logger.trace("Checking timeouts at {}", instant)
        pendingCommands.view
          .flatMap { case (commandId, trackingData) =>
            if (trackingData.commandTimeout.isBefore(instant)) {
              pendingCommands -= commandId
              logger.info(
                s"Command {} (command timeout {}) timed out at checkpoint {}.",
                commandId,
                trackingData.commandTimeout,
                instant,
              )
              List(
                Ctx(
                  trackingData.context,
                  Left(
                    CompletionResponse.TimeoutResponse(
                      commandId = trackingData.commandId
                    )
                  ),
                )
              )
            } else {
              Nil
            }
          }
          .to(immutable.Seq)
      }

      private def getOutputForCompletion(completion: Completion) = {
        val (commandId, errorText) = {
          completion.status match {
            case Some(StatusProto(code, _, _, _)) if code == Status.Code.OK.value =>
              completion.commandId -> "successful completion of command"
            case _ =>
              completion.commandId -> "failed completion of command"
          }
        }

        logger.trace("Handling {} {}", errorText, completion.commandId: Any)
        pendingCommands.remove(commandId).map { t =>
          Ctx(t.context, tracker.CompletionResponse(completion))
        }
      }

      private def getOutputForTerminalStatusCode(
          commandId: String,
          status: StatusProto,
      ): Option[Ctx[Context, Either[CompletionFailure, CompletionSuccess]]] = {
        logger.trace("Handling failure of command {}", commandId)
        pendingCommands
          .remove(commandId)
          .map { t =>
            Ctx(t.context, tracker.CompletionResponse(Completion(commandId, Some(status))))
          }
          .orElse {
            logger.trace("Platform signaled failure for unknown command {}", commandId)
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
  private val nonTerminalCodes =
    Set(Status.Code.UNKNOWN, Status.Code.INTERNAL, Status.Code.OK)
}
