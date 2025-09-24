// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command.interactive

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import com.daml.ledger.api.v2.interactive.interactive_submission_service as proto
import com.daml.scalautil.future.FutureConversion.CompletionStageConversionOps
import com.digitalasset.base.error.ErrorCode.LoggedApiException
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.LfTimestamp
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.interactive.InteractiveSubmissionEnricher
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService.{
  ExecuteRequest,
  PrepareRequest as PrepareRequestInternal,
}
import com.digitalasset.canton.ledger.api.validation.GetPreferredPackagesRequestValidator.PackageVettingRequirements
import com.digitalasset.canton.ledger.api.{Commands as ApiCommands, PackageReference, SubmissionId}
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors.InteractiveSubmissionExecuteError
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.SubmissionResult
import com.digitalasset.canton.ledger.participant.state.index.ContractStore
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace.*
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.PackagePreferenceBackend
import com.digitalasset.canton.platform.apiserver.SeedService
import com.digitalasset.canton.platform.apiserver.execution.{
  CommandExecutionResult,
  CommandExecutor,
}
import com.digitalasset.canton.platform.apiserver.services.command.interactive.codec.ExternalTransactionProcessor
import com.digitalasset.canton.platform.apiserver.services.{
  ErrorCause,
  RejectionGenerators,
  logging,
}
import com.digitalasset.canton.platform.config.InteractiveSubmissionServiceConfig
import com.digitalasset.canton.protocol.hash.HashTracer
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.{Spanning, TraceContext}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.TryUtil
import com.digitalasset.daml.lf.command.ApiCommand
import com.digitalasset.daml.lf.crypto
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

private[apiserver] object InteractiveSubmissionServiceImpl {

  def createApiService(
      submissionSyncService: state.SyncService,
      seedService: SeedService,
      commandExecutor: CommandExecutor,
      metrics: LedgerApiServerMetrics,
      checkOverloaded: TraceContext => Option[state.SubmissionResult],
      interactiveSubmissionEnricher: InteractiveSubmissionEnricher,
      config: InteractiveSubmissionServiceConfig,
      contractStore: ContractStore,
      packagePreferenceBackend: PackagePreferenceBackend,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      tracer: Tracer,
  ): InteractiveSubmissionService & AutoCloseable = new InteractiveSubmissionServiceImpl(
    submissionSyncService,
    seedService,
    commandExecutor,
    metrics,
    checkOverloaded,
    interactiveSubmissionEnricher,
    config,
    contractStore,
    packagePreferenceBackend,
    loggerFactory,
  )

}

private[apiserver] final class InteractiveSubmissionServiceImpl private[services] (
    syncService: state.SyncService,
    seedService: SeedService,
    commandExecutor: CommandExecutor,
    metrics: LedgerApiServerMetrics,
    checkOverloaded: TraceContext => Option[state.SubmissionResult],
    interactiveSubmissionEnricher: InteractiveSubmissionEnricher,
    config: InteractiveSubmissionServiceConfig,
    contractStore: ContractStore,
    packagePreferenceService: PackagePreferenceBackend,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext, tracer: Tracer)
    extends InteractiveSubmissionService
    with AutoCloseable
    with Spanning
    with NamedLogging {

  private val externalTransactionProcessor = new ExternalTransactionProcessor(
    interactiveSubmissionEnricher,
    contractStore,
    syncService,
    loggerFactory,
  )

  override def prepare(
      request: PrepareRequestInternal
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): FutureUnlessShutdown[proto.PrepareSubmissionResponse] =
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

      implicit val errorLoggingContext: ErrorLoggingContext =
        ErrorLoggingContext.fromOption(
          logger,
          loggingContext,
          request.commands.submissionId.map(SubmissionId.unwrap),
        )

      evaluateAndHash(
        seedService.nextSeed(),
        request.commands,
        request.verboseHashing,
        request.maxRecordTime,
      )
    }

  private def evaluateAndHash(
      submissionSeed: crypto.Hash,
      commands: ApiCommands,
      verboseHashing: Boolean,
      maxRecordTime: Option[LfTimestamp],
  )(implicit
      loggingContext: LoggingContextWithTrace,
      errorLoggingContext: ErrorLoggingContext,
  ): FutureUnlessShutdown[proto.PrepareSubmissionResponse] = {
    val result: EitherT[FutureUnlessShutdown, RpcError, proto.PrepareSubmissionResponse] = for {
      commandExecutionResult <- withSpan("InteractiveSubmissionService.evaluate") { _ => _ =>
        val synchronizerState = syncService.getRoutingSynchronizerState
        commandExecutor
          .execute(
            commands = commands,
            submissionSeed = submissionSeed,
            routingSynchronizerState = synchronizerState,
            forExternallySigned = true,
          )
          .leftFlatMap { errCause =>
            metrics.commands.failedCommandInterpretations.mark()
            EitherT.right[RpcError](failedOnCommandProcessing(errCause))
          }
      }
      hashTracer: HashTracer =
        if (config.enableVerboseHashing && verboseHashing)
          HashTracer.StringHashTracer(traceSubNodes = true)
        else
          HashTracer.NoOp
      prepareResult <- externalTransactionProcessor
        .processPrepare(
          commandExecutionResult,
          commands,
          config.contractLookupParallelism,
          hashTracer,
          maxRecordTime,
        )
        .leftWiden[RpcError]
      hashingDetails = hashTracer match {
        // If we have a NoOp tracer but verboseHashing was requested, it means it's disabled on the participant
        // Return a message to explain that
        case HashTracer.NoOp if verboseHashing =>
          Some(
            "Verbose hashing is disabled on this participant. Contact the node administrator for more details."
          )
        case HashTracer.NoOp => None
        case stringTracer: HashTracer.StringHashTracer => Some(stringTracer.result)
      }
    } yield proto.PrepareSubmissionResponse(
      preparedTransaction = Some(prepareResult.transaction),
      preparedTransactionHash = prepareResult.hash.unwrap,
      hashingSchemeVersion = prepareResult.hashVersion.toLAPIProto,
      hashingDetails = hashingDetails,
    )

    result.value.map(_.leftMap(_.asGrpcError).toTry).flatMap(FutureUnlessShutdown.fromTry)
  }

  private def failedOnCommandProcessing(
      error: ErrorCause
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): FutureUnlessShutdown[CommandExecutionResult] =
    FutureUnlessShutdown.failed(
      RejectionGenerators
        .commandExecutorError(error)
        .asGrpcError
    )

  override def close(): Unit = ()

  private def submitIfNotOverloaded(executionResult: CommandExecutionResult)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[SubmissionResult] =
    checkOverloaded(loggingContext.traceContext) match {
      case Some(submissionResult) => Future.successful(submissionResult)
      case None => submitTransaction(executionResult)
    }

  private def submitTransaction(result: CommandExecutionResult)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[state.SubmissionResult] = {
    metrics.commands.validSubmissions.mark()
    logger.trace("Submitting transaction to ledger.")
    syncService
      .submitTransaction(
        result.commandInterpretationResult.transaction,
        result.synchronizerRank,
        result.routingSynchronizerState,
        result.commandInterpretationResult.submitterInfo,
        result.commandInterpretationResult.transactionMeta,
        result.commandInterpretationResult.interpretationTimeNanos,
        result.commandInterpretationResult.globalKeyMapping,
        result.commandInterpretationResult.processedDisclosedContracts,
      )
      .toScalaUnwrapped
  }

  private def handleSubmissionResult(result: Try[state.SubmissionResult])(implicit
      loggingContext: LoggingContextWithTrace
  ): Try[Unit] = {
    import state.SubmissionResult.*
    result match {
      case Success(Acknowledged) =>
        logger.debug("Interactive submission acknowledged by sync-service.")
        TryUtil.unit

      case Success(result: SynchronousError) =>
        logger.info(s"Rejected: ${result.description}")
        Failure(result.exception)

      // Do not log again on errors that are logging on creation
      case Failure(error: LoggedApiException) => Failure(error)
      case Failure(error) =>
        logger.info(s"Rejected: ${error.getMessage}")
        Failure(error)
    }
  }

  override def execute(
      executionRequest: ExecuteRequest
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): FutureUnlessShutdown[proto.ExecuteSubmissionResponse] = {
    val commandIdLogging =
      executionRequest.preparedTransaction.metadata
        .flatMap(_.submitterInfo.map(_.commandId))
        .map(logging.commandId)
        .toList

    withEnrichedLoggingContext(
      logging.submissionId(executionRequest.submissionId),
      commandIdLogging *,
    ) { implicit loggingContext =>
      logger.info(
        s"Requesting execution of daml transaction with submission ID ${executionRequest.submissionId}"
      )
      val result = for {
        executionResult <- externalTransactionProcessor.processExecute(executionRequest)
        _ <- EitherT
          .liftF[Future, InteractiveSubmissionExecuteError.Reject, Unit](
            submitIfNotOverloaded(executionResult)
              .transform(handleSubmissionResult)
          )
          .mapK(FutureUnlessShutdown.outcomeK)
      } yield proto.ExecuteSubmissionResponse()

      result.value.map(_.leftMap(_.asGrpcError).toTry).flatMap(FutureUnlessShutdown.fromTry)
    }
  }

  override def getPreferredPackages(
      packageVettingRequirements: PackageVettingRequirements,
      synchronizerId: Option[SynchronizerId],
      vettingValidAt: Option[CantonTimestamp],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): FutureUnlessShutdown[Either[String, (Seq[PackageReference], SynchronizerId)]] =
    packagePreferenceService
      .getPreferredPackages(
        packageVettingRequirements = packageVettingRequirements,
        packageFilter = PackagePreferenceBackend.AllowAllPackageIds,
        synchronizerId = synchronizerId,
        vettingValidAt = vettingValidAt,
      )
}
