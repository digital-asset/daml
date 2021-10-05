// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import com.daml.api.util.TimeProvider
import com.daml.ledger.api.domain.{LedgerId, Commands => ApiCommands}
import com.daml.ledger.api.messages.command.submission.SubmitRequest
import com.daml.ledger.api.{DeduplicationPeriod, SubmissionIdGenerator}
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.index.v2._
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.crypto
import com.daml.lf.data.Ref
import com.daml.lf.engine.{Error => LfError}
import com.daml.lf.interpretation.{Error => InterpretationError}
import com.daml.lf.transaction.SubmittedTransaction
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.apiserver.configuration.LedgerConfigurationSubscription
import com.daml.platform.apiserver.error.RejectionGenerators.ErrorCauseExport
import com.daml.platform.apiserver.error.{CorrelationId, LedgerApiErrors, RejectionGenerators}
import com.daml.platform.apiserver.execution.{CommandExecutionResult, CommandExecutor}
import com.daml.platform.apiserver.{ErrorCodesVersionSwitcher, SeedService}
import com.daml.platform.server.api.services.domain.CommandSubmissionService
import com.daml.platform.server.api.services.grpc.GrpcCommandSubmissionService
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.services.time.TimeProviderType
import com.daml.platform.store.ErrorCause
import com.daml.telemetry.TelemetryContext
import com.daml.timer.Delayed
import io.grpc.StatusRuntimeException

import java.time.{Duration, Instant}
import java.util.UUID
import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

private[apiserver] object ApiSubmissionService {

  def create(
      ledgerId: LedgerId,
      writeService: state.WriteService,
      submissionService: IndexSubmissionService,
      partyManagementService: IndexPartyManagementService,
      timeProvider: TimeProvider,
      timeProviderType: TimeProviderType,
      ledgerConfigurationSubscription: LedgerConfigurationSubscription,
      seedService: SeedService,
      commandExecutor: CommandExecutor,
      configuration: ApiSubmissionService.Configuration,
      metrics: Metrics,
      errorCodesVersionSwitcher: ErrorCodesVersionSwitcher,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): GrpcCommandSubmissionService with GrpcApiService =
    new GrpcCommandSubmissionService(
      service = new ApiSubmissionService(
        writeService,
        submissionService,
        partyManagementService,
        timeProvider,
        timeProviderType,
        ledgerConfigurationSubscription,
        seedService,
        commandExecutor,
        configuration,
        metrics,
        errorCodesVersionSwitcher,
      ),
      ledgerId = ledgerId,
      currentLedgerTime = () => timeProvider.getCurrentTime,
      currentUtcTime = () => Instant.now,
      maxDeduplicationTime = () =>
        ledgerConfigurationSubscription.latestConfiguration().map(_.maxDeduplicationTime),
      submissionIdGenerator = SubmissionIdGenerator.Random,
      metrics = metrics,
    )

  final case class Configuration(
      implicitPartyAllocation: Boolean,
      enableDeduplication: Boolean,
  )

}

private[apiserver] final class ApiSubmissionService private[services] (
    writeService: state.WriteService,
    submissionService: IndexSubmissionService,
    partyManagementService: IndexPartyManagementService,
    timeProvider: TimeProvider,
    timeProviderType: TimeProviderType,
    ledgerConfigurationSubscription: LedgerConfigurationSubscription,
    seedService: SeedService,
    commandExecutor: CommandExecutor,
    configuration: ApiSubmissionService.Configuration,
    metrics: Metrics,
    errorCodesVersionSwitcher: ErrorCodesVersionSwitcher,
)(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends CommandSubmissionService
    with ErrorFactories
    with AutoCloseable {

  private val logger = ContextualizedLogger.get(this.getClass)

  override def submit(
      request: SubmitRequest
  )(implicit telemetryContext: TelemetryContext): Future[Unit] =
    withEnrichedLoggingContext(logging.commands(request.commands)) { implicit loggingContext =>
      logger.info("Submitting transaction")
      logger.trace(s"Commands: ${request.commands.commands.commands}")
      val evaluatedCommand = ledgerConfigurationSubscription
        .latestConfiguration() match {
        case Some(ledgerConfiguration) =>
          if (writeService.isApiDeduplicationEnabled && configuration.enableDeduplication) {
            deduplicateAndRecordOnLedger(
              seedService.nextSeed(),
              request.commands,
              ledgerConfiguration,
            )
          } else {
            evaluateAndSubmit(seedService.nextSeed(), request.commands, ledgerConfiguration)
              .transform(handleSubmissionResult)
          }
        case None =>
          errorCodesVersionSwitcher.chooseAsFailedFuture(
            v1 = ErrorFactories.missingLedgerConfig(definiteAnswer = Some(false)),
            v2 = LedgerApiErrors.InterpreterErrors.LookupErrors.LedgerConfigurationNotFound
              .Reject(cause = "The ledger configuration is not available.")(
                correlationId = CorrelationId.none,
                loggingContext = loggingContext,
                logger = logger,
              )
              .asGrpcError,
          )
      }
      evaluatedCommand
        .andThen(logger.logErrorsOnCall[Unit])
    }

  private def deduplicateAndRecordOnLedger(
      seed: crypto.Hash,
      commands: ApiCommands,
      ledgerConfig: Configuration,
  )(implicit loggingContext: LoggingContext, telemetryContext: TelemetryContext): Future[Unit] =
    submissionService
      .deduplicateCommand(
        commands.commandId,
        commands.actAs.toList,
        commands.submittedAt,
        DeduplicationPeriod.deduplicateUntil(
          commands.submittedAt,
          commands.deduplicationPeriod,
        ),
      )
      .flatMap {
        case CommandDeduplicationNew =>
          evaluateAndSubmit(seed, commands, ledgerConfig)
            .transform(handleSubmissionResult)
            .recoverWith { case NonFatal(originalCause) =>
              submissionService
                .stopDeduplicatingCommand(commands.commandId, commands.actAs.toList)
                .transform(_ => Failure(originalCause))
            }
        case _: CommandDeduplicationDuplicate =>
          metrics.daml.commands.deduplicatedCommands.mark()
          errorCodesVersionSwitcher.chooseAsFailedFuture(
            v1 = {
              val exception = duplicateCommandException
              logger.debug(exception.getMessage)
              exception
            },
            v2 = RejectionGenerators.duplicateCommand(
              logger = logger,
              loggingContext = loggingContext,
              correlationId = CorrelationId.none,
            ),
          )
      }

  private def handleSubmissionResult(result: Try[state.SubmissionResult])(implicit
      loggingContext: LoggingContext
  ): Try[Unit] = {
    import state.SubmissionResult._
    result match {
      case Success(Acknowledged) =>
        logger.debug("Success")
        Success(())

      case Success(result: SynchronousError) =>
        logger.info(s"Rejected: ${result.description}")
        Failure(result.exception)

      case Failure(error) =>
        logger.info(s"Rejected: ${error.getMessage}")
        Failure(error)
    }
  }

  private def handleCommandExecutionResult(
      result: Either[ErrorCause, CommandExecutionResult]
  ): Future[CommandExecutionResult] = {

    result.fold(
      error => {
        metrics.daml.commands.failedCommandInterpretations.mark()
        errorCodesVersionSwitcher.chooseAsFailedFuture(
          v1 = toStatusExceptionV1(error),
          v2 = RejectionGenerators
            .commandExecutorError(cause = ErrorCauseExport.fromErrorCause(error))(
              logger = logger,
              loggingContext = implicitly[LoggingContext],
              correlationId = CorrelationId.none,
            ),
        )
      },
      Future.successful,
    )

  }

  private def evaluateAndSubmit(
      submissionSeed: crypto.Hash,
      commands: ApiCommands,
      ledgerConfig: Configuration,
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): Future[state.SubmissionResult] =
    for {
      result <- commandExecutor.execute(commands, submissionSeed, ledgerConfig)
      transactionInfo <- handleCommandExecutionResult(result)
      partyAllocationResults <- allocateMissingInformees(transactionInfo.transaction)
      submissionResult <- submitTransaction(transactionInfo, partyAllocationResults, ledgerConfig)
    } yield submissionResult

  // Takes the whole transaction to ensure to traverse it only if necessary
  private[services] def allocateMissingInformees(
      transaction: SubmittedTransaction
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): Future[Seq[state.SubmissionResult]] =
    if (configuration.implicitPartyAllocation) {
      val partiesInTransaction = transaction.informees.toSeq
      for {
        fetchedParties <- partyManagementService.getParties(partiesInTransaction)
        knownParties = fetchedParties.iterator.map(_.party).toSet
        missingParties = partiesInTransaction.filterNot(knownParties)
        submissionResults <- Future.sequence(missingParties.map(allocateParty))
      } yield submissionResults
    } else Future.successful(Seq.empty)

  private def allocateParty(
      name: Ref.Party
  )(implicit telemetryContext: TelemetryContext): Future[state.SubmissionResult] = {
    val submissionId = Ref.SubmissionId.assertFromString(UUID.randomUUID().toString)
    withEnrichedLoggingContext(logging.party(name), logging.submissionId(submissionId)) {
      implicit loggingContext =>
        logger.info("Implicit party allocation")
        writeService
          .allocateParty(
            hint = Some(name),
            displayName = Some(name),
            submissionId = submissionId,
          )
    }
  }.toScala

  private def submitTransaction(
      transactionInfo: CommandExecutionResult,
      partyAllocationResults: Seq[state.SubmissionResult],
      ledgerConfig: Configuration,
  )(implicit telemetryContext: TelemetryContext): Future[state.SubmissionResult] =
    partyAllocationResults.find(_ != state.SubmissionResult.Acknowledged) match {
      case Some(result) =>
        Future.successful(result)
      case None =>
        timeProviderType match {
          case TimeProviderType.WallClock =>
            // Submit transactions such that they arrive at the ledger sequencer exactly when record time equals ledger time.
            // If the ledger time of the transaction is far in the future (farther than the expected latency),
            // the submission to the WriteService is delayed.
            val submitAt = transactionInfo.transactionMeta.ledgerEffectiveTime.toInstant
              .minus(ledgerConfig.timeModel.avgTransactionLatency)
            val submissionDelay = Duration.between(timeProvider.getCurrentTime, submitAt)
            if (submissionDelay.isNegative)
              submitTransaction(transactionInfo)
            else {
              logger.info(s"Delaying submission by $submissionDelay")
              metrics.daml.commands.delayedSubmissions.mark()
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
  )(implicit telemetryContext: TelemetryContext): Future[state.SubmissionResult] = {
    metrics.daml.commands.validSubmissions.mark()
    writeService
      .submitTransaction(
        result.submitterInfo,
        result.transactionMeta,
        result.transaction,
        result.interpretationTimeNanos,
      )
      .toScala
  }

  /** This method encodes logic related to legacy error codes (V1).
    * Cf. self-service error codes (V2) in //ledger/error
    */
  def toStatusExceptionV1(errorCause: ErrorCause): StatusRuntimeException =
    errorCause match {
      case cause @ ErrorCause.DamlLf(error: LfError) =>
        error match {
          case LfError.Interpretation(
                LfError.Interpretation.DamlException(
                  InterpretationError.ContractNotFound(_) |
                  InterpretationError.DuplicateContractKey(_)
                ),
                _,
              ) | LfError.Validation(LfError.Validation.ReplayMismatch(_)) =>
            ErrorFactories.aborted(cause.explain, definiteAnswer = Some(false))
          case _ =>
            ErrorFactories.invalidArgument(definiteAnswer = Some(false))(cause.explain)
        }
      case cause: ErrorCause.LedgerTime =>
        ErrorFactories.aborted(cause.explain, definiteAnswer = Some(false))
    }

  override def close(): Unit = ()

}
