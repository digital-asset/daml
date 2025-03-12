// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command.interactive

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.daml.ledger.api.v2.interactive.interactive_submission_service.*
import com.daml.scalautil.future.FutureConversion.CompletionStageConversionOps
import com.daml.timer.Delayed
import com.digitalasset.base.error.ErrorCode.LoggedApiException
import com.digitalasset.base.error.{ContextualizedErrorLogger, DamlError}
import com.digitalasset.canton.crypto.InteractiveSubmission
import com.digitalasset.canton.crypto.InteractiveSubmission.TransactionMetadataForHashing
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService.{
  ExecuteRequest,
  PrepareRequest as PrepareRequestInternal,
}
import com.digitalasset.canton.ledger.api.util.TimeProvider
import com.digitalasset.canton.ledger.api.{Commands as ApiCommands, SubmissionId}
import com.digitalasset.canton.ledger.configuration.LedgerTimeModel
import com.digitalasset.canton.ledger.error.CommonErrors.ServerIsShuttingDown
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.{SubmissionResult, SynchronizerRank}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
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
  CommandInterpretationResult,
}
import com.digitalasset.canton.platform.apiserver.services.command.interactive.InteractiveSubmissionServiceImpl.ExecutionTimes
import com.digitalasset.canton.platform.apiserver.services.{
  ErrorCause,
  RejectionGenerators,
  TimeProviderType,
  logging,
}
import com.digitalasset.canton.platform.config.InteractiveSubmissionServiceConfig
import com.digitalasset.canton.platform.store.dao.events.LfValueTranslation
import com.digitalasset.canton.protocol.hash.HashTracer
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.{Spanning, TraceContext, Traced}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.TryUtil
import com.digitalasset.canton.version.{HashingSchemeVersion, ProtocolVersion}
import com.digitalasset.daml.lf.command.ApiCommand
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.{ImmArray, Time}
import com.digitalasset.daml.lf.transaction.SubmittedTransaction
import io.opentelemetry.api.trace.Tracer

import java.time.{Duration, Instant}
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

private[apiserver] object InteractiveSubmissionServiceImpl {

  private final case class ExecutionTimes(
      ledgerEffective: Time.Timestamp,
      submitAt: Option[Instant],
  )

  def createApiService(
      submissionSyncService: state.SyncService,
      timeProvider: TimeProvider,
      timeProviderType: TimeProviderType,
      seedService: SeedService,
      commandExecutor: CommandExecutor,
      metrics: LedgerApiServerMetrics,
      checkOverloaded: TraceContext => Option[state.SubmissionResult],
      lfValueTranslation: LfValueTranslation,
      config: InteractiveSubmissionServiceConfig,
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
    lfValueTranslation,
    config,
    loggerFactory,
  )

}

private[apiserver] final class InteractiveSubmissionServiceImpl private[services] (
    syncService: state.SyncService,
    timeProvider: TimeProvider,
    timeProviderType: TimeProviderType,
    seedService: SeedService,
    commandExecutor: CommandExecutor,
    metrics: LedgerApiServerMetrics,
    checkOverloaded: TraceContext => Option[state.SubmissionResult],
    lfValueTranslation: LfValueTranslation,
    config: InteractiveSubmissionServiceConfig,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext, tracer: Tracer)
    extends InteractiveSubmissionService
    with AutoCloseable
    with Spanning
    with NamedLogging {

  private val transactionEncoder = new PreparedTransactionEncoder(loggerFactory)
  private val transactionDecoder = new PreparedTransactionDecoder(loggerFactory)

  override def prepare(
      request: PrepareRequestInternal
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): FutureUnlessShutdown[PrepareSubmissionResponse] =
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

      evaluateAndHash(seedService.nextSeed(), request.commands, request.verboseHashing)
    }

  private def evaluateAndHash(
      submissionSeed: crypto.Hash,
      commands: ApiCommands,
      verboseHashing: Boolean,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      errorLoggingContext: ContextualizedErrorLogger,
  ): FutureUnlessShutdown[PrepareSubmissionResponse] = {
    val result: EitherT[FutureUnlessShutdown, DamlError, PrepareSubmissionResponse] = for {
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
            EitherT.right[DamlError](failedOnCommandProcessing(errCause))
          }
      }

      preEnrichedCommandInterpretationResult = commandExecutionResult.commandInterpretationResult

      // We need to enrich the transaction before computing the hash, because record labels must be part of the hash
      enrichedTransaction <- EitherT
        .liftF(
          lfValueTranslation
            .enrichVersionedTransaction(preEnrichedCommandInterpretationResult.transaction)
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      // Same with input contracts
      enrichedDisclosedContracts <- EitherT
        .liftF(
          preEnrichedCommandInterpretationResult.processedDisclosedContracts.toList
            .parTraverse { contract =>
              lfValueTranslation
                .enrichCreateNode(contract.create)
                .map(enrichedCreate => contract.copy(create = enrichedCreate))
            }
            .map(ImmArray.from)
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      commandInterpretationResult = preEnrichedCommandInterpretationResult.copy(
        transaction = SubmittedTransaction(enrichedTransaction),
        processedDisclosedContracts = enrichedDisclosedContracts,
      )
      // Require this participant to be connected to the synchronizer on which the transaction will be run
      synchronizerId = commandExecutionResult.synchronizerRank.synchronizerId
      protocolVersionForChosenSynchronizer <- EitherT.fromEither[FutureUnlessShutdown](
        protocolVersionForSynchronizerId(synchronizerId)
      )
      // Use the highest hashing versions supported on that protocol version
      hashVersion = HashingSchemeVersion
        .getHashingSchemeVersionsForProtocolVersion(protocolVersionForChosenSynchronizer)
        .max1
      transactionUUID = UUID.randomUUID()
      mediatorGroup = 0
      preparedTransaction <- EitherT
        .liftF(
          transactionEncoder.serializeCommandInterpretationResult(
            commandInterpretationResult,
            synchronizerId,
            // TODO(i20688) Transaction UUID should be picked in the TransactionConfirmationRequestFactory
            transactionUUID,
            // TODO(i20688) Mediator group should be picked in the ProtocolProcessor
            mediatorGroup,
          )
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      metadataForHashing = TransactionMetadataForHashing.createFromDisclosedContracts(
        commandInterpretationResult.submitterInfo.actAs.toSet,
        commandInterpretationResult.submitterInfo.commandId,
        transactionUUID,
        mediatorGroup,
        synchronizerId,
        Option.when(commandInterpretationResult.dependsOnLedgerTime)(
          commandInterpretationResult.transactionMeta.ledgerEffectiveTime
        ),
        commandInterpretationResult.transactionMeta.submissionTime,
        commandInterpretationResult.processedDisclosedContracts,
      )
      hashTracer: HashTracer =
        if (config.enableVerboseHashing && verboseHashing)
          HashTracer.StringHashTracer(traceSubNodes = true)
        else
          HashTracer.NoOp
      transactionHash <- EitherT
        .fromEither[FutureUnlessShutdown](
          InteractiveSubmission.computeVersionedHash(
            hashVersion,
            commandInterpretationResult.transaction,
            metadataForHashing,
            commandInterpretationResult.transactionMeta.optNodeSeeds
              .map(_.toList.toMap)
              .getOrElse(Map.empty),
            protocolVersionForChosenSynchronizer,
            hashTracer,
          )
        )
        .leftMap(err =>
          CommandExecutionErrors.InteractiveSubmissionPreparationError
            .Reject(s"Failed to compute hash: $err"): DamlError
        )
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
    } yield PrepareSubmissionResponse(
      preparedTransaction = Some(preparedTransaction),
      preparedTransactionHash = transactionHash.unwrap,
      hashingSchemeVersion = hashVersion.toLAPIProto,
      hashingDetails = hashingDetails,
    )

    result.value.map(_.leftMap(_.asGrpcError).toTry).flatMap(FutureUnlessShutdown.fromTry)
  }

  private def failedOnCommandProcessing(
      error: ErrorCause
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): FutureUnlessShutdown[CommandExecutionResult] =
    FutureUnlessShutdown.failed(
      RejectionGenerators
        .commandExecutorError(error)
        .asGrpcError
    )

  override def close(): Unit = ()

  private def submitIfNotOverloaded(
      transactionInfo: CommandInterpretationResult,
      submitAt: Option[Instant],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[SubmissionResult] =
    checkOverloaded(loggingContext.traceContext) match {
      case Some(submissionResult) => Future.successful(submissionResult)
      case None => submitTransactionAt(transactionInfo, submitAt)
    }

  private def submitTransaction(
      commandInterpretationResult: CommandInterpretationResult
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[state.SubmissionResult] = {
    metrics.commands.validSubmissions.mark()

    logger.trace("Submitting transaction to ledger.")

    val routingSynchronizerState = syncService.getRoutingSynchronizerState

    syncService
      .selectRoutingSynchronizer(
        submitterInfo = commandInterpretationResult.submitterInfo,
        optSynchronizerId = commandInterpretationResult.optSynchronizerId,
        transactionMeta = commandInterpretationResult.transactionMeta,
        transaction = commandInterpretationResult.transaction,
        disclosedContractIds =
          commandInterpretationResult.processedDisclosedContracts.map(_.contractId).toList,
        transactionUsedForExternalSigning = true,
        routingSynchronizerState = routingSynchronizerState,
      )
      .map(FutureUnlessShutdown.pure)
      .leftMap(err => FutureUnlessShutdown.failed[SynchronizerRank](err.asGrpcError))
      .merge
      .flatten
      .failOnShutdownTo(ServerIsShuttingDown.Reject().asGrpcError)
      .flatMap { synchronizerRank =>
        syncService
          .submitTransaction(
            transaction = commandInterpretationResult.transaction,
            synchronizerRank = synchronizerRank,
            routingSynchronizerState = routingSynchronizerState,
            submitterInfo = commandInterpretationResult.submitterInfo,
            transactionMeta = commandInterpretationResult.transactionMeta,
            _estimatedInterpretationCost = commandInterpretationResult.interpretationTimeNanos,
            keyResolver = commandInterpretationResult.globalKeyMapping,
            processedDisclosedContracts = commandInterpretationResult.processedDisclosedContracts,
          )
          .toScalaUnwrapped
      }
  }

  private def protocolVersionForSynchronizerId(
      synchronizerId: SynchronizerId
  )(implicit loggingContext: LoggingContextWithTrace): Either[DamlError, ProtocolVersion] =
    syncService
      .getProtocolVersionForSynchronizer(Traced(synchronizerId))
      .toRight(
        CommandExecutionErrors.InteractiveSubmissionPreparationError
          .Reject(s"Unknown synchronizer id $synchronizerId")
      )

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

  private def submitTransactionAt(
      transactionInfo: CommandInterpretationResult,
      submitAt: Option[Instant],
  )(implicit loggingContext: LoggingContextWithTrace): Future[state.SubmissionResult] = {
    val delayO = submitAt.map(Duration.between(timeProvider.getCurrentTime, _))
    delayO match {
      case Some(delay) if delay.isNegative =>
        submitTransaction(transactionInfo)
      case Some(delay) =>
        logger.info(s"Delaying submission by $delay")
        metrics.commands.delayedSubmissions.mark()
        val scalaDelay = scala.concurrent.duration.Duration.fromNanos(delay.toNanos)
        Delayed.Future.by(scalaDelay)(submitTransaction(transactionInfo))
      case None =>
        submitTransaction(transactionInfo)
    }
  }

  /** @param ledgerEffectiveTimeO
    *   set if the ledger effective time was used when preparing the transaction
    */
  private def deriveExecutionTimes(ledgerEffectiveTimeO: Option[Time.Timestamp]): ExecutionTimes =
    (ledgerEffectiveTimeO, timeProviderType) match {
      case (Some(let), _) =>
        // Submit transactions such that they arrive at the ledger sequencer exactly when record time equals
        // ledger time. If the ledger time of the transaction is far in the future (farther than the expected
        // latency), the submission to the SyncService is delayed.
        val submitAt =
          let.toInstant.minus(LedgerTimeModel.maximumToleranceTimeModel.avgTransactionLatency)
        ExecutionTimes(let, Some(submitAt))
      case (None, _) =>
        // If the ledger effective time was not set in the request, it means the transaction is assumed not to use time.
        // So we use the current time here at submission time.
        ExecutionTimes(Time.Timestamp.assertFromInstant(timeProvider.getCurrentTime), None)
    }

  override def execute(
      executionRequest: ExecuteRequest
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): FutureUnlessShutdown[ExecuteSubmissionResponse] = FutureUnlessShutdown.outcomeF {
    val commandIdLogging =
      executionRequest.preparedTransaction.metadata
        .flatMap(_.submitterInfo.map(_.commandId))
        .map(logging.commandId)
        .toList

    withEnrichedLoggingContext(
      logging.submissionId(executionRequest.submissionId),
      commandIdLogging*
    ) { implicit loggingContext =>
      logger.info(
        s"Requesting execution of daml transaction with submission ID ${executionRequest.submissionId}"
      )
      for {
        ledgerEffectiveTimeO <- transactionDecoder.extractLedgerEffectiveTime(executionRequest)
        times = deriveExecutionTimes(ledgerEffectiveTimeO)
        deserializationResult <- transactionDecoder.makeCommandExecutionResult(
          executionRequest,
          times.ledgerEffective,
        )
        _ <- submitIfNotOverloaded(deserializationResult.commandExecutionResult, times.submitAt)
          .transform(handleSubmissionResult)
      } yield ExecuteSubmissionResponse()

    }
  }
}
