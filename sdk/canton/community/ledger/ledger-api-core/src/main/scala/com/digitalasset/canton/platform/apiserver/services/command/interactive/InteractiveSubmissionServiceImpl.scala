// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command.interactive

import cats.data.EitherT
import cats.implicits.toBifunctorOps
import cats.syntax.either.*
import com.daml.error.{ContextualizedErrorLogger, DamlError}
import com.daml.ledger.api.v2.interactive.interactive_submission_service.*
import com.daml.scalautil.future.FutureConversion.*
import com.daml.timer.Delayed
import com.digitalasset.canton.CommandId
import com.digitalasset.canton.crypto.InteractiveSubmission
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
import com.digitalasset.canton.platform.apiserver.services.command.interactive.InteractiveSubmissionServiceImpl.ExecutionTimes
import com.digitalasset.canton.platform.apiserver.services.{
  ErrorCause,
  RejectionGenerators,
  TimeProviderType,
  logging,
}
import com.digitalasset.canton.platform.store.dao.events.LfValueTranslation
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{Spanning, TraceContext, Traced}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.version.{HashingSchemeVersion, ProtocolVersion}
import com.digitalasset.daml.lf.command.ApiCommand
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.Time
import io.opentelemetry.api.trace.Tracer

import java.time.{Duration, Instant}
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

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
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext, tracer: Tracer)
    extends InteractiveSubmissionService
    with AutoCloseable
    with Spanning
    with NamedLogging {

  private val transactionEncoder = new PreparedTransactionEncoder(loggerFactory, lfValueTranslation)
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
  ): Future[PrepareSubmissionResponse] = {
    val result: EitherT[Future, DamlError, PrepareSubmissionResponse] = for {
      commandExecutionResultE <- withSpan("InteractiveSubmissionService.evaluate") { _ => _ =>
        EitherT.liftF(commandExecutor.execute(commands, submissionSeed))
      }
      commandExecutionResult <- EitherT.liftF(handleCommandExecutionResult(commandExecutionResultE))
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
        .getVersionedHashConstructorFor(protocolVersionForChosenDomain)
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
      transactionHash <- EitherT
        .fromEither[Future](
          InteractiveSubmission.computeVersionedHash(
            hashVersion,
            CommandId(commandExecutionResult.submitterInfo.commandId),
          )
        )
        .leftMap(err =>
          CommandExecutionErrors.InteractiveSubmissionPreparationError
            .Reject(s"Failed to compute hash: $err")
        )
        .leftWiden[DamlError]
    } yield PrepareSubmissionResponse(
      preparedTransaction = Some(preparedTransaction),
      preparedTransactionHash = transactionHash.getCryptographicEvidence,
      hashingSchemeVersion = hashVersion.toLAPIProto,
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

  private def submitIfNotOverloaded(
      transactionInfo: CommandExecutionResult,
      submitAt: Option[Instant],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[SubmissionResult] =
    checkOverloaded(loggingContext.traceContext) match {
      case Some(submissionResult) => Future.successful(submissionResult)
      case None => submitTransactionAt(transactionInfo, submitAt)
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

  private def protocolVersionForDomainId(
      domainId: DomainId
  )(implicit loggingContext: LoggingContextWithTrace): Either[DamlError, ProtocolVersion] =
    syncService
      .getProtocolVersionForDomain(Traced(domainId))
      .toRight(
        CommandExecutionErrors.InteractiveSubmissionPreparationError
          .Reject(s"Unknown domain ID $domainId")
      )

  private def submitTransactionAt(
      transactionInfo: CommandExecutionResult,
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

  /** @param ledgerEffectiveTimeO - set if the ledger effective time was used when preparing the transaction */
  private def deriveExecutionTimes(ledgerEffectiveTimeO: Option[Time.Timestamp]): ExecutionTimes =
    (ledgerEffectiveTimeO, timeProviderType) match {
      case (Some(let), TimeProviderType.WallClock) =>
        // Submit transactions such that they arrive at the ledger sequencer exactly when record time equals
        // ledger time. If the ledger time of the transaction is far in the future (farther than the expected
        // latency), the submission to the SyncService is delayed.
        val submitAt =
          let.toInstant.minus(LedgerTimeModel.maximumToleranceTimeModel.avgTransactionLatency)
        ExecutionTimes(let, Some(submitAt))
      case (Some(let), _) =>
        ExecutionTimes(let, None)
      case (None, _) =>
        ExecutionTimes(Time.Timestamp.assertFromInstant(timeProvider.getCurrentTime), None)
    }

  override def execute(
      executionRequest: ExecuteRequest
  )(implicit loggingContext: LoggingContextWithTrace): Future[ExecuteSubmissionResponse] = {
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
      } yield ExecuteSubmissionResponse()

    }
  }
}
