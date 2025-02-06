// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command.interactive

import cats.data.EitherT
import cats.implicits.toBifunctorOps
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.daml.error.ErrorCode.LoggedApiException
import com.daml.error.{ContextualizedErrorLogger, DamlError}
import com.daml.ledger.api.v2.interactive.interactive_submission_service.*
import com.daml.scalautil.future.FutureConversion.*
import com.daml.timer.Delayed
import com.digitalasset.canton.crypto.InteractiveSubmission
import com.digitalasset.canton.crypto.InteractiveSubmission.TransactionMetadataForHashing
import com.digitalasset.canton.ledger.api.domain.{Commands as ApiCommands, SubmissionId}
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService.{
  ExecuteRequest,
  PrepareRequest as PrepareRequestInternal,
}
import com.digitalasset.canton.ledger.api.util.TimeProvider
import com.digitalasset.canton.ledger.configuration.LedgerTimeModel
import com.digitalasset.canton.ledger.error.groups.{CommandExecutionErrors, RequestValidationErrors}
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.SubmissionResult
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
import com.digitalasset.canton.platform.apiserver.services.command.interactive.PreparedTransactionDecoder.DeserializationResult
import com.digitalasset.canton.platform.apiserver.services.{
  ErrorCause,
  RejectionGenerators,
  TimeProviderType,
  logging,
}
import com.digitalasset.canton.platform.config.InteractiveSubmissionServiceConfig
import com.digitalasset.canton.platform.store.dao.events.LfValueTranslation
import com.digitalasset.canton.protocol.hash.HashTracer
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{Spanning, TraceContext, Traced}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.version.{HashingSchemeVersion, ProtocolVersion}
import com.digitalasset.daml.lf.command.ApiCommand
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.transaction.SubmittedTransaction
import io.opentelemetry.api.trace.Tracer

import java.time.Duration
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

private[apiserver] object InteractiveSubmissionServiceImpl {
  def createApiService(
      writeService: state.WriteService,
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
    writeService,
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
    writeService: state.WriteService,
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

      evaluateAndHash(seedService.nextSeed(), request.commands, request.verboseHashing)
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
      verboseHashing: Boolean,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      errorLoggingContext: ContextualizedErrorLogger,
  ): Future[PrepareSubmissionResponse] = {
    val result: EitherT[Future, DamlError, PrepareSubmissionResponse] = for {
      commandExecutionResultE <- withSpan("InteractiveSubmissionService.evaluate") { _ => _ =>
        EitherT.liftF(commandExecutor.execute(commands, submissionSeed))
      }
      preEnrichedCommandExecutionResult <- EitherT.liftF(
        handleCommandExecutionResult(commandExecutionResultE)
      )
      // We need to enrich the transaction before computing the hash, because record labels must be part of the hash
      enrichedTransaction <- EitherT.liftF(
        lfValueTranslation
          .enrichVersionedTransaction(preEnrichedCommandExecutionResult.transaction)
      )
      // Same with input contracts
      enrichedDisclosedContracts <- EitherT.liftF(
        preEnrichedCommandExecutionResult.processedDisclosedContracts.toList
          .parTraverse { contract =>
            lfValueTranslation
              .enrichCreateNode(contract.create)
              .map(enrichedCreate => contract.copy(create = enrichedCreate))
          }
          .map(ImmArray.from)
      )
      commandExecutionResult = preEnrichedCommandExecutionResult.copy(
        transaction = SubmittedTransaction(enrichedTransaction),
        processedDisclosedContracts = enrichedDisclosedContracts,
      )
      // Domain is required to be explicitly provided for now
      domainId <- EitherT.fromEither[Future](
        commandExecutionResult.optDomainId.toRight(
          RequestValidationErrors.MissingField.Reject("domain_id")
        )
      )
      // Require this participant to be connected to the domain on which the transaction will be run
      protocolVersionForChosenDomain <- EitherT.fromEither[Future](
        protocolVersionForDomainId(domainId)
      )
      // Use the highest hashing versions supported on that protocol version
      hashVersion = HashingSchemeVersion
        .getHashingSchemeVersionsForProtocolVersion(protocolVersionForChosenDomain)
        .max1
      transactionUUID = UUID.randomUUID()
      mediatorGroup = 0
      preparedTransaction <- EitherT.liftF(
        transactionEncoder.serializeCommandExecutionResult(
          commandExecutionResult,
          // TODO(i20688) Domain ID should be picked by the domain router
          domainId,
          // TODO(i20688) Transaction UUID should be picked in the TransactionConfirmationRequestFactory
          transactionUUID,
          // TODO(i20688) Mediator group should be picked in the ProtocolProcessor
          mediatorGroup,
        )
      )
      metadataForHashing = TransactionMetadataForHashing.createFromDisclosedContracts(
        commandExecutionResult.submitterInfo.actAs.toSet,
        commandExecutionResult.submitterInfo.commandId,
        transactionUUID,
        mediatorGroup,
        domainId,
        Option.when(commandExecutionResult.dependsOnLedgerTime)(
          commandExecutionResult.transactionMeta.ledgerEffectiveTime
        ),
        commandExecutionResult.transactionMeta.submissionTime,
        commandExecutionResult.processedDisclosedContracts,
      )
      hashTracer: HashTracer =
        if (config.enableVerboseHashing && verboseHashing)
          HashTracer.StringHashTracer(traceSubNodes = true)
        else
          HashTracer.NoOp
      transactionHash <- EitherT
        .fromEither[Future](
          InteractiveSubmission.computeVersionedHash(
            hashVersion,
            commandExecutionResult.transaction,
            metadataForHashing,
            commandExecutionResult.transactionMeta.optNodeSeeds
              .map(_.toList.toMap)
              .getOrElse(Map.empty),
            protocolVersionForChosenDomain,
            hashTracer,
          )
        )
        .leftMap(err =>
          CommandExecutionErrors.InteractiveSubmissionPreparationError
            .Reject(s"Failed to compute hash: $err")
        )
        .leftWiden[DamlError]
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

    result.value.map(_.leftMap(_.asGrpcError).toTry).flatMap(Future.fromTry)
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
  ): Future[state.SubmissionResult] = {
    val submitAt = transactionInfo.transactionMeta.ledgerEffectiveTime.toInstant
      .minus(LedgerTimeModel.maximumToleranceTimeModel.avgTransactionLatency)
    val submissionDelay = Duration.between(timeProvider.getCurrentTime, submitAt)
    if (submissionDelay.isNegative)
      submitTransaction(transactionInfo)
    else {
      logger.info(s"Delaying submission by $submissionDelay")
      metrics.commands.delayedSubmissions.mark()
      val scalaDelay = scala.concurrent.duration.Duration.fromNanos(submissionDelay.toNanos)
      Delayed.Future.by(scalaDelay)({ submitTransaction(transactionInfo) })
    }
  }
  private def submitTransaction(
      result: CommandExecutionResult
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[state.SubmissionResult] = {
    metrics.commands.validSubmissions.mark()
    logger.trace("Submitting transaction to ledger.")
    writeService
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

  private def protocolVersionForDomainId(
      domainId: DomainId
  )(implicit loggingContext: LoggingContextWithTrace): Either[DamlError, ProtocolVersion] =
    writeService
      .getProtocolVersionForDomain(Traced(domainId))
      .toRight(
        CommandExecutionErrors.InteractiveSubmissionPreparationError
          .Reject(s"Unknown domain ID $domainId")
      )

  private def handleSubmissionResult(result: Try[state.SubmissionResult])(implicit
      loggingContext: LoggingContextWithTrace
  ): Try[Unit] = {
    import state.SubmissionResult.*
    result match {
      case Success(Acknowledged) =>
        logger.debug("Interactive submission acknowledged by sync-service.")
        Success(())

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
      request: ExecuteRequest
  )(implicit loggingContext: LoggingContextWithTrace): Future[ExecuteSubmissionResponse] = {
    val commandIdLogging =
      request.preparedTransaction.metadata
        .flatMap(_.submitterInfo.map(_.commandId))
        .map(logging.commandId)
        .toList

    withEnrichedLoggingContext(
      logging.submissionId(request.submissionId),
      commandIdLogging*
    ) { implicit loggingContext =>
      logger.info(
        s"Requesting execution of daml transaction with submission ID ${request.submissionId}"
      )
      val result = for {
        deserializationResult <- EitherT.liftF[Future, DamlError, DeserializationResult](
          transactionDecoder.makeCommandExecutionResult(request)
        )
        _ <- EitherT.liftF[Future, DamlError, Unit](
          submitIfNotOverloaded(deserializationResult.commandExecutionResult)
            .transform(handleSubmissionResult)
        )
      } yield ExecuteSubmissionResponse()

      result.value.map(_.leftMap(_.asGrpcError).toTry).flatMap(Future.fromTry)
    }
  }
}
