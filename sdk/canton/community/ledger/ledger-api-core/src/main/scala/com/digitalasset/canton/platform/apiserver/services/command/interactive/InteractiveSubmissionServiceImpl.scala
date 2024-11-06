// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command.interactive

import cats.implicits.toBifunctorOps
import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.interactive_submission_data.PreparedTransaction
import com.daml.ledger.api.v2.interactive_submission_service.*
import com.daml.scalautil.future.FutureConversion.*
import com.daml.timer.Delayed
import com.digitalasset.canton.CommandId
import com.digitalasset.canton.crypto.{Hash as CantonHash, InteractiveSubmission}
import com.digitalasset.canton.ledger.api.domain.{Commands as ApiCommands, SubmissionId}
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService.{
  ExecuteRequest,
  PrepareRequest as PrepareRequestInternal,
}
import com.digitalasset.canton.ledger.api.util.TimeProvider
import com.digitalasset.canton.ledger.configuration.LedgerTimeModel
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.SubmissionResult
import com.digitalasset.canton.ledger.participant.state.SubmitterInfo.ExternallySignedSubmission
import com.digitalasset.canton.logging.LoggingContextWithTrace.*
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
import com.digitalasset.canton.platform.apiserver.services.command.interactive.InteractiveSubmissionServiceImpl.PendingRequest
import com.digitalasset.canton.platform.apiserver.services.{
  ErrorCause,
  RejectionGenerators,
  TimeProviderType,
  logging,
}
import com.digitalasset.canton.platform.config.InteractiveSubmissionServiceConfig
import com.digitalasset.canton.platform.store.dao.events.LfValueTranslation
import com.digitalasset.canton.tracing.{Spanning, TraceContext}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.daml.lf.command.ApiCommand
import com.digitalasset.daml.lf.crypto
import com.github.benmanes.caffeine.cache
import com.github.benmanes.caffeine.cache.Caffeine
import com.google.protobuf.ByteString
import io.grpc.StatusRuntimeException
import io.opentelemetry.api.trace.Tracer
import monocle.macros.syntax.lens.*

import java.time.Duration
import scala.concurrent.{ExecutionContext, Future}

private[apiserver] object InteractiveSubmissionServiceImpl {
  private final case class PendingRequest(
      executionResult: CommandExecutionResult,
      commands: ApiCommands,
  )

  def createApiService(
      submissionSyncService: state.SubmissionSyncService,
      timeProvider: TimeProvider,
      timeProviderType: TimeProviderType,
      seedService: SeedService,
      commandExecutor: CommandExecutor,
      metrics: LedgerApiServerMetrics,
      checkOverloaded: TraceContext => Option[state.SubmissionResult],
      interactiveSubmissionServiceConfig: InteractiveSubmissionServiceConfig,
      lfValueTranslation: LfValueTranslation,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      tracer: Tracer,
  ): InteractiveSubmissionService & AutoCloseable = new InteractiveSubmissionServiceImpl(
    submissionSyncService,
    timeProvider,
    timeProviderType,
    seedService,
    commandExecutor,
    metrics,
    checkOverloaded,
    interactiveSubmissionServiceConfig,
    lfValueTranslation,
    loggerFactory,
  )

}

private[apiserver] final class InteractiveSubmissionServiceImpl private[services] (
    syncService: state.SubmissionSyncService,
    timeProvider: TimeProvider,
    timeProviderType: TimeProviderType,
    seedService: SeedService,
    commandExecutor: CommandExecutor,
    metrics: LedgerApiServerMetrics,
    checkOverloaded: TraceContext => Option[state.SubmissionResult],
    interactiveSubmissionServiceConfig: InteractiveSubmissionServiceConfig,
    lfValueTranslation: LfValueTranslation,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext, tracer: Tracer)
    extends InteractiveSubmissionService
    with AutoCloseable
    with Spanning
    with NamedLogging {

  // TODO(i20660): This is temporary while we settle on a proper serialization format for the prepared transaction
  // For now for simplicity we keep the required data in-memory to be able to submit the transaction when
  // it is submitted with external signatures. Obviously this restricts usage of the prepare / submit
  // flow to using the same node, and does not survive restarts of the node.
  private val pendingPrepareRequests: cache.Cache[ByteString, PendingRequest] = Caffeine
    .newBuilder()
    // Max duration to keep the prepared transaction in memory after it's been prepared
    .expireAfterWrite(Duration.ofHours(1))
    .build[ByteString, PendingRequest]()

  private val transactionEncoder = new PreparedTransactionEncoder(loggerFactory, lfValueTranslation)

  override def prepare(
      request: PrepareRequestInternal
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[PrepareSubmissionResponse] =
    withEnrichedLoggingContext(logging.commands(request.commands)) { implicit loggingContext =>
      logger.info(
        s"Requesting preparation of daml transaction with command ID ${request.commands.commandId}"
      )
      val cmds = request.commands.commands.commands
      // TODO(i20726): make sure this does not leak information
      logger.debug(
        show"Submitted commands for prepare are: ${if (cmds.length > 1) "\n  " else ""}${cmds
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
            .mkString("\n  ")}"
      )

      implicit val errorLoggingContext: ContextualizedErrorLogger =
        ErrorLoggingContext.fromOption(
          logger,
          loggingContext,
          request.commands.submissionId.map(SubmissionId.unwrap),
        )

      evaluateAndHash(seedService.nextSeed(), request.commands)
    }

  private def handleCommandExecutionResult(
      result: Either[ErrorCause, CommandExecutionResult]
  )(implicit contextualizedErrorLogger: ContextualizedErrorLogger): Future[CommandExecutionResult] =
    result.fold(
      error => {
        metrics.commands.failedCommandInterpretations.mark()
        failedOnCommandExecution(error)
      },
      Future.successful,
    )

  // TODO(i20660): We hash the command ID only for now while the proper hashing algorithm is being designed
  private def computeTransactionHash(preparedTransaction: PreparedTransaction)(implicit
      errorLoggingContext: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, CantonHash] =
    for {
      metadata <- preparedTransaction.metadata.toRight(
        RequestValidationErrors.MissingField.Reject("metadata").asGrpcError
      )
      submitterInfo <- metadata.submitterInfo.toRight(
        RequestValidationErrors.MissingField.Reject("submitter_info").asGrpcError
      )
      cmdId <- CommandId
        .fromProtoPrimitive(submitterInfo.commandId)
        .leftMap(err => RequestValidationErrors.InvalidField.Reject("command_id", err).asGrpcError)
    } yield InteractiveSubmission.computeHash(cmdId)

  private def evaluateAndHash(
      submissionSeed: crypto.Hash,
      commands: ApiCommands,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      errorLoggingContext: ContextualizedErrorLogger,
  ): Future[PrepareSubmissionResponse] =
    for {
      result <- withSpan("InteractiveSubmissionService.evaluate") { _ => _ =>
        commandExecutor.execute(commands, submissionSeed)
      }
      transactionInfo <- handleCommandExecutionResult(result)
      preparedTransaction <- transactionEncoder.serializeCommandExecutionResult(transactionInfo)
      transactionHash <- Future.fromTry(computeTransactionHash(preparedTransaction).toTry)
      // Caffeine doesn't have putIfAbsent. Use `get` with a function to insert the value in the cache to obtain the same result
      _ = pendingPrepareRequests.get(
        transactionHash.getCryptographicEvidence,
        _ => PendingRequest(transactionInfo, commands),
      )
    } yield PrepareSubmissionResponse(
      preparedTransaction = Some(preparedTransaction),
      preparedTransactionHash = transactionHash.getCryptographicEvidence,
    )

  private def failedOnCommandExecution(
      error: ErrorCause
  )(implicit contextualizedErrorLogger: ContextualizedErrorLogger): Future[CommandExecutionResult] =
    Future.failed(
      RejectionGenerators
        .commandExecutorError(error)
        .asGrpcError
    )

  override def close(): Unit = ()

  private def submitIfNotOverloaded(transactionInfo: CommandExecutionResult)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[SubmissionResult] =
    checkOverloaded(loggingContext.traceContext) match {
      case Some(submissionResult) => Future.successful(submissionResult)
      case None => submitTransactionWithDelay(transactionInfo)
    }

  private def submitTransactionWithDelay(
      transactionInfo: CommandExecutionResult
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[state.SubmissionResult] =
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

  private def submitTransaction(
      result: CommandExecutionResult
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[state.SubmissionResult] = {
    metrics.commands.validSubmissions.mark()
    logger.trace("Submitting transaction to ledger.")
    syncService
      .submitTransaction(
        result.submitterInfo,
        result.optDomainId,
        result.transactionMeta,
        result.transaction,
        result.interpretationTimeNanos,
        result.globalKeyMapping,
        result.processedDisclosedContracts,
      )
      .toScalaUnwrapped
  }

  override def execute(
      request: ExecuteRequest
  )(implicit loggingContext: LoggingContextWithTrace): Future[ExecuteSubmissionResponse] = {
    // TODO(i20660) add command id to logging
    val additionalLoggingEntries = Seq(
      request.workflowId.map(logging.workflowId)
    ).flatten
    withEnrichedLoggingContext(
      logging.submissionId(request.submissionId),
      additionalLoggingEntries*
    ) { implicit loggingContext =>
      logger.info(
        s"Requesting execution of daml transaction with submission ID ${request.submissionId}"
      )
      for {
        hash <- Future.fromTry(computeTransactionHash(request.preparedTransaction).toTry)
        pending <- Option(
          pendingPrepareRequests.getIfPresent(hash.getCryptographicEvidence)
        )
          .map(Future.successful)
          .getOrElse(
            // This doesn't use a pre-defined error code because it's a temporary workaround
            // while we need to keep the transaction in memory
            Future.failed(
              io.grpc.Status.NOT_FOUND
                .withDescription("Unknown command")
                .asRuntimeException()
            )
          )
        // Update the pending transaction with input data from the execute request
        updatedPending =
          pending
            .focus(_.executionResult.submitterInfo.submissionId)
            .replace(Some(request.submissionId))
            .focus(_.executionResult.transactionMeta.workflowId)
            .replace(request.workflowId)
            .focus(_.executionResult.submitterInfo.deduplicationPeriod)
            .replace(request.deduplicationPeriod)
            .focus(_.executionResult.submitterInfo.externallySignedSubmission)
            .replace(Some(ExternallySignedSubmission(hash, request.signatures)))
        _ <- submitIfNotOverloaded(updatedPending.executionResult)
      } yield ExecuteSubmissionResponse()
    }
  }
}
