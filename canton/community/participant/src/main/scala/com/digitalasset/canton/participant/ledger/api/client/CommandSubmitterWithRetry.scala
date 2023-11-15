// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api.client

import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.commands.Commands
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.error.ErrorCodeUtils
import com.digitalasset.canton.ledger.client.services.commands.SynchronousCommandClient
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.DelayUtil
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.google.rpc.code.Code
import com.google.rpc.status.Status
import io.grpc.protobuf.StatusProto

import scala.concurrent.duration.{Duration, FiniteDuration, *}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure

/** Wraps a synchronous command client with the ability to retry commands that failed with retryable errors
  *  up to a given max number of retries.
  */
class CommandSubmitterWithRetry(
    maxRetries: Int,
    synchronousCommandClient: SynchronousCommandClient,
    futureSupervisor: FutureSupervisor,
    protected override val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
    overrideRetryable: PartialFunction[Status, Boolean] = PartialFunction.empty,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseableAsync {

  private val DefaultRetryableDelay = 1.second

  /** Submits commands and retries timed out ones (at most the amount given by `maxRetries`)
    *
    * @param commands to be submitted
    * @return Future with the result of the submission. The result can signal success, failure, max retries reached
    *         or aborted due to shutdown.
    */
  def submitCommands(
      commands: Commands,
      timeout: Duration,
  )(implicit traceContext: TraceContext): Future[CommandResult] =
    submitCommandsInternal(commands, timeout).unwrap
      .map {
        case UnlessShutdown.Outcome(commandResult) => commandResult
        case UnlessShutdown.AbortedDueToShutdown => CommandResult.AbortedDueToShutdown
      }
      .thereafter {
        case Failure(ex) =>
          logger.warn(s"Command failed [${commands.commandId}] badly due to an exception", ex)
        case _ => ()
      }

  private def submitCommandsInternal(commands: Commands, timeout: Duration)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[CommandResult] = {
    val commandId = commands.commandId

    def go(retriesLeft: Int): FutureUnlessShutdown[CommandResult] =
      runWithAbortOnShutdown(submitAndWait(commands, timeout))
        .flatMap {
          case Left(status) =>
            getErrorRetryability(status)
              .map { retryAfter =>
                if (retriesLeft > 0) {
                  logger.info(
                    s"Command with id = $commandId failed with a retryable error. Retrying (attempt ${maxRetries - retriesLeft + 1}/$maxRetries)"
                  )
                  DelayUtil
                    .delayIfNotClosing("command-retry-delay", retryAfter, this)
                    .flatMap(_ => performUnlessClosingUSF("retry-command")(go(retriesLeft - 1)))
                } else {
                  logger.info(
                    s"Command with id = $commandId failed after reaching max retries of $maxRetries."
                  )
                  FutureUnlessShutdown.pure(CommandResult.MaxRetriesReached(commandId, status))
                }
              }
              .getOrElse {
                logger.info(s"Command with id = $commandId failed.")
                FutureUnlessShutdown.pure(CommandResult.Failed(commandId, status))
              }
          case Right(transactionId) =>
            FutureUnlessShutdown.pure(CommandResult.Success(transactionId))
        }

    go(maxRetries)
  }

  /** Decides on error retryabillity and if positive, the backoff delay to be followed until retry:
    * * `overrideRetryable` has highest precedence
    * * then, the extracted error category
    * * and last, whether an error is a DEADLINE_EXCEEDED status. We look for DEADLINE_EXCEEDED
    * since it can be returned directly by the gRPC server and does not conform to our
    * self-service error codes shape.
    */
  private def getErrorRetryability(status: Status): Option[FiniteDuration] =
    overrideRetryable
      .andThen(retryableOverride => Option.when(retryableOverride)(DefaultRetryableDelay))
      .applyOrElse(
        status,
        { (status: Status) =>
          ErrorCodeUtils
            .errorCategoryFromString(status.message)
            .flatMap(_.retryable)
            .map(_.duration)
            .orElse(Option.when(status.code == Code.DEADLINE_EXCEEDED.value)(DefaultRetryableDelay))
        },
      )

  private def runWithAbortOnShutdown(f: => Future[Either[Status, String]])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[Status, String]] = {
    val promise = new PromiseUnlessShutdown[Either[Status, String]](
      description = "submit-command-with-retry",
      futureSupervisor = futureSupervisor,
    )

    val taskId = runOnShutdown(promise)
    promise.completeWith(FutureUnlessShutdown.outcomeF(f))
    promise.futureUS.thereafter(_ => cancelShutdownTask(taskId))
  }

  private def submitAndWait(
      commands: Commands,
      timeout: Duration,
  )(implicit traceContext: TraceContext): Future[Either[Status, String]] =
    synchronousCommandClient
      .submitAndWaitForTransactionId(SubmitAndWaitRequest(Some(commands)), timeout = Some(timeout))
      .map(response => Right(response.transactionId))
      .recoverWith { case throwable =>
        Option(StatusProto.fromThrowable(throwable))
          .map(Status.fromJavaProto)
          .map(errStatus => Future.successful(Left(errStatus)))
          .getOrElse {
            logger.info(s"Could not decode submission result error", throwable)
            Future.failed(throwable)
          }
      }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = Nil
}

sealed trait CommandResult extends PrettyPrinting with Product with Serializable

object CommandResult {
  final case class Success(transactionId: String) extends CommandResult {
    override def pretty: Pretty[Success.this.type] = prettyOfClass(
      param("transactionId", _.transactionId.doubleQuoted)
    )
  }

  final case class Failed(commandId: String, errorStatus: Status) extends CommandResult {
    override def pretty: Pretty[Failed] = prettyOfClass(
      param("commandId", _.commandId.doubleQuoted),
      param("errorStatus", _.errorStatus),
    )
  }

  final case object AbortedDueToShutdown extends CommandResult {
    override def pretty: Pretty[AbortedDueToShutdown.this.type] =
      prettyOfObject[AbortedDueToShutdown.this.type]
  }

  final case class MaxRetriesReached(commandId: String, lastErrorStatus: Status)
      extends CommandResult {
    override def pretty: Pretty[MaxRetriesReached] = prettyOfClass(
      param("commandId", _.commandId.doubleQuoted),
      param("lastError", _.lastErrorStatus),
    )
  }
}

object CommandSubmitterWithRetry {

  def isSuccess(status: Status): Boolean = status.code == Code.OK.value

  def isRetryable(
      status: Status,
      overrideRetryable: PartialFunction[Status, Boolean] = PartialFunction.empty,
  ): Boolean =
    overrideRetryable.applyOrElse(
      status,
      (s: Status) => ErrorCodeUtils.errorCategoryFromString(s.message).exists(_.retryable.nonEmpty),
    )
}
