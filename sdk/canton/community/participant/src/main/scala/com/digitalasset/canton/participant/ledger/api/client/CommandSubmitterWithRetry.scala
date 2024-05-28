// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api.client

import com.daml.ledger.api.v2.commands.Commands
import com.digitalasset.canton.concurrent.{DirectExecutionContext, FutureSupervisor}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.client.LedgerClientUtils
import com.digitalasset.canton.ledger.client.services.commands.CommandServiceClient
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{DelayUtil, LoggerUtil}
import com.google.rpc.status.Status
import io.grpc.Context

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.Failure

/** Wraps a synchronous command client with the ability to retry commands that failed with retryable errors */
class CommandSubmitterWithRetry(
    commandServiceClient: CommandServiceClient,
    clock: Clock,
    futureSupervisor: FutureSupervisor,
    protected override val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
    decideRetry: Status => Option[FiniteDuration] = LedgerClientUtils.defaultRetryRules,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseableAsync {

  private val directEc = DirectExecutionContext(logger)

  // abort retries if the remaining timeout is less than this (in case there is little chance that the command will succeed)
  // if our descendants ever get to lower latencies, they should lower this value as well
  private val DEFAULT_MINIMUM_DEADLINE = 10.millis

  /** Submits commands and retries timed out ones (at most the amount given by `maxRetries`)
    *
    * @param commands to be submitted
    * @return Future with the result of the submission. The result can signal success, failure, max retries reached
    *         or aborted due to shutdown.
    */
  def submitCommands(
      commands: Commands,
      timeout: FiniteDuration,
  )(implicit traceContext: TraceContext): Future[CommandResult] =
    submitCommandsInternal(commands, timeout).unwrap
      .map {
        case UnlessShutdown.Outcome(commandResult) => commandResult
        case UnlessShutdown.AbortedDueToShutdown => CommandResult.AbortedDueToShutdown
      }
      .thereafter {
        case Failure(ex) =>
          logger.error(s"Command failed [${commands.commandId}] badly due to an exception", ex)
        case _ => ()
      }

  /** Aborts a future immediately if this object is closing.
    *
    * For very long running futures where you don't want to wait for it to complete in case of
    * an abort, you can wrap it here into a future unless shutdown.
    */
  def abortIfClosing[R](name: String, futureSupervisor: FutureSupervisor)(
      future: => Future[R]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[R] = {
    if (isClosing) FutureUnlessShutdown.abortedDueToShutdown
    else {
      implicit val ec: ExecutionContext = directEc
      val promise = new PromiseUnlessShutdown[R](
        description = name,
        futureSupervisor = futureSupervisor,
      )
      val taskId = runOnShutdown(promise)
      promise.completeWith(FutureUnlessShutdown.outcomeF(future))
      promise.futureUS.thereafter(_ => cancelShutdownTask(taskId))
    }
  }

  private def submitCommandsInternal(commands: Commands, timeout: FiniteDuration)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[CommandResult] = {
    val commandId = commands.commandId
    val deadline: CantonTimestamp = clock.now.plus(timeout.toJava)
    def go(): FutureUnlessShutdown[CommandResult] = {
      abortIfClosing("submit-with-retry", futureSupervisor) {
        logger.debug(s"Submitting command=${commandId} to command service")
        commandServiceClient
          .submitAndWaitForUpdateId(commands, timeout = Some(timeout))
      }
        .flatMap {
          case Left(status) =>
            decideRetry(status)
              .map { retryAfter =>
                val nextAttempt =
                  clock.now.plus(retryAfter.toJava).plus(DEFAULT_MINIMUM_DEADLINE.toJava)
                if (nextAttempt < deadline) {
                  logger.info(
                    s"Command with id = $commandId failed with a retryable error ${status}. Retrying after ${LoggerUtil
                        .roundDurationForHumans(retryAfter)}"
                  )
                  DelayUtil
                    .delayIfNotClosing("command-retry-delay", retryAfter, this)
                    .flatMap(_ => go())
                } else {
                  logger.info(
                    s"Command with id = $commandId failed after reaching the deadline ${deadline}. Failure is ${status}."
                  )
                  FutureUnlessShutdown.pure(CommandResult.TimeoutReached(commandId, status))
                }
              }
              .getOrElse {
                logger.info(
                  s"Command with id = $commandId failed non-retryable with ${status}. Giving up."
                )
                if (status.code == com.google.rpc.Code.DEADLINE_EXCEEDED.getNumber) {
                  FutureUnlessShutdown.pure(CommandResult.TimeoutReached(commandId, status))
                } else {
                  FutureUnlessShutdown.pure(CommandResult.Failed(commandId, status))
                }
              }
          case Right(result) =>
            FutureUnlessShutdown.pure(CommandResult.Success(result.updateId))
        }
    }
    Context.current().fork().call(() => go())
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

  final case class TimeoutReached(commandId: String, lastErrorStatus: Status)
      extends CommandResult {
    override def pretty: Pretty[TimeoutReached] = prettyOfClass(
      param("commandId", _.commandId.doubleQuoted),
      param("lastError", _.lastErrorStatus),
    )
  }
}
