// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command

import com.daml.error.ContextualizedErrorLogger
import com.daml.error.ErrorCode.LoggedApiException
import com.daml.scalautil.future.FutureConversion.CompletionStageConversionOps
import com.daml.timer.Delayed
import com.digitalasset.canton.ledger.api.domain.{Commands as ApiCommands, SubmissionId}
import com.digitalasset.canton.ledger.api.messages.command.submission.SubmitRequest
import com.digitalasset.canton.ledger.api.services.CommandSubmissionService
import com.digitalasset.canton.ledger.api.util.TimeProvider
import com.digitalasset.canton.ledger.configuration.LedgerTimeModel
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.LoggingContextWithTrace.{
  implicitExtractTraceContext,
  withEnrichedLoggingContext,
}
import com.digitalasset.canton.logging.TracedLoggerOps.TracedLoggerOps
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.apiserver.SeedService
import com.digitalasset.canton.platform.apiserver.execution.{
  CommandExecutionResult,
  CommandExecutor,
}
import com.digitalasset.canton.platform.apiserver.services.{
  ErrorCause,
  RejectionGenerators,
  TimeProviderType,
  logging,
}
import com.digitalasset.canton.tracing.{Spanning, TraceContext}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.daml.lf.command.ApiCommand
import com.digitalasset.daml.lf.crypto
import io.opentelemetry.api.trace.Tracer

import java.time.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

private[apiserver] object CommandSubmissionServiceImpl {

  def createApiService(
      submissionSyncService: state.SubmissionSyncService,
      timeProvider: TimeProvider,
      timeProviderType: TimeProviderType,
      seedService: SeedService,
      commandExecutor: CommandExecutor,
      checkOverloaded: TraceContext => Option[state.SubmissionResult],
      metrics: LedgerApiServerMetrics,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      tracer: Tracer,
  ): CommandSubmissionService & AutoCloseable = new CommandSubmissionServiceImpl(
    submissionSyncService,
    timeProvider,
    timeProviderType,
    seedService,
    commandExecutor,
    checkOverloaded,
    metrics,
    loggerFactory,
  )

}

private[apiserver] final class CommandSubmissionServiceImpl private[services] (
    submissionSyncService: state.SubmissionSyncService,
    timeProvider: TimeProvider,
    timeProviderType: TimeProviderType,
    seedService: SeedService,
    commandExecutor: CommandExecutor,
    checkOverloaded: TraceContext => Option[state.SubmissionResult],
    metrics: LedgerApiServerMetrics,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext, tracer: Tracer)
    extends CommandSubmissionService
    with AutoCloseable
    with Spanning
    with NamedLogging {

  override def submit(
      request: SubmitRequest
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): FutureUnlessShutdown[Unit] =
    withEnrichedLoggingContext(logging.commands(request.commands)) { implicit loggingContext =>
      logger.info(
        show"Phase 1 started: Submitting commands for interpretation: ${request.commands}."
      )
      val cmds = request.commands.commands.commands
      logger.debug(show"Submitted commands are: ${if (cmds.length > 1) "\n  " else ""}${cmds
          .map {
            case ApiCommand.Create(templateRef, _) =>
              s"create ${templateRef.qName}"
            case ApiCommand.Exercise(templateRef, _, choiceId, _) =>
              s"exercise @${templateRef.qName} $choiceId"
            case ApiCommand.ExerciseByKey(templateRef, _, choiceId, _) =>
              s"exerciseByKey @${templateRef.qName} $choiceId"
            case ApiCommand.CreateAndExercise(templateRef, _, choiceId, _) =>
              s"createAndExercise ${templateRef.qName} ... $choiceId ..."
          }
          .map(_.singleQuoted)
          .toSeq
          .mkString("\n  ")}")

      implicit val errorLoggingContext: ContextualizedErrorLogger =
        ErrorLoggingContext.fromOption(
          logger,
          loggingContext,
          request.commands.submissionId.map(SubmissionId.unwrap),
        )

      val evaluatedCommand =
        evaluateAndSubmit(seedService.nextSeed(), request.commands)
          .transform(handleSubmissionResult)
      evaluatedCommand.thereafter(logger.logErrorsOnCall[UnlessShutdown[Unit]])
    }

  private def handleSubmissionResult(result: Try[UnlessShutdown[state.SubmissionResult]])(implicit
      loggingContext: LoggingContextWithTrace
  ): Try[UnlessShutdown[Unit]] = {
    import state.SubmissionResult.*
    result match {
      case Success(UnlessShutdown.Outcome(Acknowledged)) =>
        logger.debug("Submission acknowledged by sync-service.")
        Success(UnlessShutdown.unit)

      case Success(UnlessShutdown.Outcome(result: SynchronousError)) =>
        logger.info(s"Rejected: ${result.description}")
        Failure(result.exception)

      case Success(UnlessShutdown.AbortedDueToShutdown) =>
        Success(UnlessShutdown.AbortedDueToShutdown)
      // Do not log again on errors that are logging on creation
      case Failure(error: LoggedApiException) => Failure(error)
      case Failure(error) =>
        logger.info(s"Rejected: ${error.getMessage}")
        Failure(error)
    }
  }

  private def handleCommandExecutionResult(
      result: Either[ErrorCause, CommandExecutionResult]
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): FutureUnlessShutdown[CommandExecutionResult] =
    result.fold(
      error =>
        FutureUnlessShutdown.outcomeF {
          metrics.commands.failedCommandInterpretations.mark()
          failedOnCommandExecution(error)
        },
      FutureUnlessShutdown.pure,
    )

  private def evaluateAndSubmit(
      submissionSeed: crypto.Hash,
      commands: ApiCommands,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      errorLoggingContext: ContextualizedErrorLogger,
  ): FutureUnlessShutdown[state.SubmissionResult] =
    checkOverloaded(loggingContext.traceContext) match {
      case Some(submissionResult) => FutureUnlessShutdown.pure(submissionResult)
      case None =>
        for {
          result <- withSpan("ApiSubmissionService.evaluate") { _ => _ =>
            commandExecutor.execute(commands, submissionSeed)
          }
          transactionInfo <- handleCommandExecutionResult(result)
          submissionResult <- submitTransactionWithDelay(
            transactionInfo
          )
        } yield submissionResult
    }

  private def submitTransactionWithDelay(
      transactionInfo: CommandExecutionResult
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): FutureUnlessShutdown[state.SubmissionResult] = FutureUnlessShutdown.outcomeF {
    timeProviderType match {
      case TimeProviderType.WallClock =>
        // Submit transactions such that they arrive at the ledger sequencer exactly when record time equals ledger time.
        // If the ledger time of the transaction is far in the future (farther than the expected latency),
        // the submission to the SyncService is delayed.
        val submitAt = transactionInfo.transactionMeta.ledgerEffectiveTime.toInstant
          .minus(LedgerTimeModel.maximumToleranceTimeModel.avgTransactionLatency)
        val submissionDelay = Duration.between(timeProvider.getCurrentTime, submitAt)
        if (submissionDelay.isNegative)
          submitTransaction(transactionInfo)
        else {
          logger.info(s"Delaying submission by $submissionDelay")
          metrics.commands.delayedSubmissions.mark()
          val scalaDelay = scala.concurrent.duration.Duration.fromNanos(submissionDelay.toNanos)
          Delayed.Future.by(scalaDelay)(submitTransaction(transactionInfo))
        }
      case TimeProviderType.Static =>
        // In static time mode, record time is always equal to ledger time
        submitTransaction(transactionInfo)
    }
  }

  private def submitTransaction(
      result: CommandExecutionResult
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[state.SubmissionResult] = {
    metrics.commands.validSubmissions.mark()
    logger.trace("Submitting transaction to ledger.")
    submissionSyncService
      .submitTransaction(
        result.submitterInfo,
        result.optSynchronizerId,
        result.transactionMeta,
        result.transaction,
        result.interpretationTimeNanos,
        result.globalKeyMapping,
        result.processedDisclosedContracts,
      )
      .toScalaUnwrapped
  }

  private def failedOnCommandExecution(
      error: ErrorCause
  )(implicit contextualizedErrorLogger: ContextualizedErrorLogger): Future[CommandExecutionResult] =
    Future.failed(
      RejectionGenerators
        .commandExecutorError(error)
        .asGrpcError
    )

  override def close(): Unit = ()
}
