// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.interactive_submission_service.*
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.ledger.api.domain.{Commands as ApiCommands, SubmissionId}
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService.PrepareRequest as PrepareRequestInternal
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
import com.digitalasset.canton.platform.apiserver.services.command.InteractiveSubmissionServiceImpl.PendingRequest
import com.digitalasset.canton.platform.apiserver.services.{
  ErrorCause,
  RejectionGenerators,
  logging,
}
import com.digitalasset.canton.platform.config.InteractiveSubmissionServiceConfig
import com.digitalasset.canton.tracing.Spanning
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.daml.lf.command.ApiCommand
import com.digitalasset.daml.lf.crypto
import com.github.benmanes.caffeine.cache
import com.github.benmanes.caffeine.cache.Caffeine
import com.google.protobuf.ByteString
import io.opentelemetry.api.trace.Tracer

import java.time.Duration
import scala.concurrent.{ExecutionContext, Future}

private[apiserver] object InteractiveSubmissionServiceImpl {

  private final case class PendingRequest(
      executionResult: CommandExecutionResult,
      commands: ApiCommands,
  )

  def createApiService(
      seedService: SeedService,
      commandExecutor: CommandExecutor,
      metrics: LedgerApiServerMetrics,
      interactiveSubmissionServiceConfig: InteractiveSubmissionServiceConfig,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      tracer: Tracer,
  ): InteractiveSubmissionService & AutoCloseable = new InteractiveSubmissionServiceImpl(
    seedService,
    commandExecutor,
    metrics,
    interactiveSubmissionServiceConfig,
    loggerFactory,
  )

}

private[apiserver] final class InteractiveSubmissionServiceImpl private[services] (
    seedService: SeedService,
    commandExecutor: CommandExecutor,
    metrics: LedgerApiServerMetrics,
    interactiveSubmissionServiceConfig: InteractiveSubmissionServiceConfig,
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
    // We only read from it when the transaction is submitted, after which we don't need to keep it around
    // so expire it almost immediately after access
    .expireAfterAccess(Duration.ofSeconds(1))
    .build[ByteString, PendingRequest]()

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

  private def evaluateAndHash(
      submissionSeed: crypto.Hash,
      commands: ApiCommands,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      errorLoggingContext: ContextualizedErrorLogger,
  ): Future[PrepareSubmissionResponse] =
    for {
      result <- withSpan("ApiSubmissionService.evaluate") { _ => _ =>
        commandExecutor.execute(commands, submissionSeed)
      }
      transactionInfo <- handleCommandExecutionResult(result)
      // TODO(i20660): Until serialization and hashing are figured out, use the command Id as transaction and hash
      transactionByteString = ByteString.copyFromUtf8(commands.commandId.toString)
      transactionHash = Hash
        .digest(
          HashPurpose.PreparedSubmission,
          transactionByteString,
          HashAlgorithm.Sha256,
        )
        .getCryptographicEvidence
      // Caffeine doesn't have putIfAbsent. Use `get` with a function to insert the value in the cache to obtain the same result
      _ = pendingPrepareRequests.get(
        transactionHash,
        _ => PendingRequest(transactionInfo, commands),
      )
    } yield PrepareSubmissionResponse(
      preparedTransaction = transactionByteString,
      preparedTransactionHash = transactionHash,
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
}
