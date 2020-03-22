// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.services.commands.tracker

import akka.stream.stage._
import akka.stream.{Attributes, Inlet, Outlet}
import com.digitalasset.api.util.TimestampConversion.toInstant
import com.digitalasset.grpc.{GrpcException, GrpcStatus}
import com.digitalasset.ledger.api.v1.command_completion_service._
import com.digitalasset.ledger.api.v1.command_submission_service._
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.client.services.commands.CompletionStreamElement
import com.digitalasset.util.Ctx
import com.digitalasset.util.logging.Lazy
import com.google.protobuf.empty.Empty
import com.google.rpc.code._
import com.google.rpc.status.Status
import io.grpc.{Status => RpcStatus}
import org.slf4j.LoggerFactory

import scala.collection.{breakOut, immutable, mutable}
import scala.concurrent.{Future, Promise}
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success, Try}

/**
  * Implements the logic of command tracking via two streams, a submit request and command completion stream.
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
  *
  */
// TODO(mthvedt): This should have unit tests.
private[commands] class CommandTracker[Context]
    extends GraphStageWithMaterializedValue[
      CommandTrackerShape[Context],
      Future[immutable.Map[String, Context]]] {

  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  val submitRequestIn: Inlet[Ctx[Context, SubmitRequest]] =
    Inlet[Ctx[Context, SubmitRequest]]("submitRequestIn")
  val submitRequestOut: Outlet[Ctx[(Context, String), SubmitRequest]] =
    Outlet[Ctx[(Context, String), SubmitRequest]]("submitRequestOut")
  val commandResultIn: Inlet[Either[Ctx[(Context, String), Try[Empty]], CompletionStreamElement]] =
    Inlet[Either[Ctx[(Context, String), Try[Empty]], CompletionStreamElement]]("commandResultIn")
  val resultOut: Outlet[Ctx[Context, Completion]] =
    Outlet[Ctx[Context, Completion]]("resultOut")
  val offsetOut: Outlet[LedgerOffset] =
    Outlet[LedgerOffset]("offsetOut")

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes): (GraphStageLogic, Future[Map[String, Context]]) = {

    val promise = Promise[immutable.Map[String, Context]]

    val logic: TimerGraphStageLogic = new TimerGraphStageLogic(shape) {

      private val pendingCommands = new mutable.HashMap[String, TrackingData[Context]]()

      setHandler(
        submitRequestOut,
        new OutHandler {
          override def onPull(): Unit = pull(submitRequestIn)

          override def onDownstreamFinish(cause: Throwable): Unit = {
            cancel(submitRequestIn)
            completeStageIfTerminal()
          }
        }
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
        }
      )

      setHandler(resultOut, new OutHandler {
        override def onPull(): Unit = pull(commandResultIn)
      })

      setHandler(
        commandResultIn,
        new InHandler {

          /**
            * This port was pulled by [[resultOut]], so that port expects an output.
            * If processing the input produces one, we push it through [[resultOut]], otherwise we pull this port again.
            * If multiple outputs are produced (possible with timeouts only) we get rid of them with emitMultiple.
            */
          override def onPush(): Unit = {
            grab(commandResultIn) match {
              case Left(submitResponse) =>
                val syncFailure = handleSubmitResponse(submitResponse)
                syncFailure.fold(pull(commandResultIn))(push(resultOut, _))

              case Right(CompletionStreamElement.CompletionElement(completion)) =>
                val result = getOutputForCompletion(completion)
                result.fold(pull(commandResultIn))(push(resultOut, _))

              case Right(CompletionStreamElement.CheckpointElement(checkpoint)) =>
                val timeouts = getOutputForTimeout(checkpoint)
                if (timeouts.nonEmpty) emitMultiple(resultOut, timeouts)
                else pull(commandResultIn)

                checkpoint.offset.foreach(emit(offsetOut, _))
            }

            completeStageIfTerminal()
          }
        }
      )

      setHandler(
        offsetOut,
        new OutHandler {
          override def onPull(): Unit =
            () //nothing to do here as the offset stream will be read with constant demand, storing the latest element
        }
      )

      private def completeStageIfTerminal(): Unit = {
        if (isClosed(submitRequestIn) && pendingCommands.isEmpty) {
          completeStage()
        }
      }

      import CommandTracker.nonTerminalCodes

      private def handleSubmitResponse(submitResponse: Ctx[(Context, String), Try[Empty]]) = {
        val Ctx((_, commandId), value) = submitResponse
        value match {
          case Failure(GrpcException(status @ GrpcStatus(code, _), _)) if !nonTerminalCodes(code) =>
            getOutputForTerminalStatusCode(commandId, GrpcStatus.toProto(status))
          case Failure(throwable) =>
            logger.warn(
              s"Service responded with error for submitting command with context ${submitResponse.context}. Status of command is unknown. watching for completion...",
              throwable
            )
            None
          case Success(_) =>
            logger.trace("Received confirmation that command {} was accepted.", commandId)
            None
        }
      }

      private def registerSubmission(submitRequest: Ctx[Context, SubmitRequest]): Unit = {
        submitRequest.value.commands
          .fold(
            throw new IllegalArgumentException(
              "Commands field is missing from received SubmitRequest in CommandTracker")
            with NoStackTrace) { commands =>
            val commandId = commands.commandId
            logger.trace("Begin tracking of command {}", commandId)
            if (pendingCommands.get(commandId).nonEmpty) {
              // TODO return an error identical to the server side duplicate command error once that's defined.
              throw new IllegalStateException(
                s"A command with id $commandId is already being tracked. CommandIds submitted to the CommandTracker must be unique.")
              with NoStackTrace
            }
            pendingCommands += (commandId ->
              TrackingData(
                commandId,
                toInstant(commands.getMaximumRecordTime),
                submitRequest.value.traceContext,
                submitRequest.context))
          }
        ()
      }

      @SuppressWarnings(Array("org.wartremover.warts.Any"))
      private def getOutputForTimeout(checkpoint: Checkpoint) = {
        logger.trace(
          "Handling checkpoint {}",
          Lazy(s"${checkpoint.offset} at ${toInstant(checkpoint.getRecordTime)}"))
        val mrtBoundary = toInstant(checkpoint.getRecordTime)
        pendingCommands.flatMap {
          case (commandId, trackingData) =>
            if (trackingData.maximumRecordTime.isBefore(mrtBoundary)) {
              pendingCommands -= commandId
              logger.info(
                s"Command {} (mrt {}) timed out at checkpoint {}.",
                commandId,
                trackingData.maximumRecordTime,
                mrtBoundary)
              List(
                Ctx(
                  trackingData.context,
                  Completion(
                    trackingData.commandId,
                    Some(
                      com.google.rpc.status.Status(RpcStatus.ABORTED.getCode.value(), "Timeout")),
                    traceContext = trackingData.traceContext)
                ))
            } else {
              Nil
            }
        }(breakOut)
      }

      private def getOutputForCompletion(completion: Completion) = {
        val (commandId, errorText) = {
          completion.status match {
            case Some(status) if Code.fromValue(status.code) == Code.OK =>
              completion.commandId -> "successful completion of command"
            case _ =>
              completion.commandId -> "failed completion of command"
          }
        }

        logger.trace("Handling {} {}", errorText, completion.commandId: Any)
        pendingCommands.remove(commandId).map { t =>
          Ctx(t.context, completion)
        }
      }

      private def getOutputForTerminalStatusCode(
          commandId: String,
          status: Status): Option[Ctx[Context, Completion]] = {
        logger.trace("Handling failure of command {}", commandId)
        pendingCommands
          .remove(commandId)
          .map { t =>
            Ctx(t.context, Completion(commandId, Some(status), traceContext = t.traceContext))
          }
          .orElse {
            logger.trace("Platform signaled failure for unknown command {}", commandId)
            None
          }
      }

      override def postStop(): Unit = {
        promise.tryComplete(Success(pendingCommands.map {
          case (k, v) => k -> v.context
        }(breakOut)))
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
    Set(RpcStatus.Code.UNKNOWN, RpcStatus.Code.INTERNAL, RpcStatus.Code.OK)
}
